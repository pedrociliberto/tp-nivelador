package client

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()

	if err := sendAndReceiveBets(client.conn, client.config.AgencyId); err != nil {
		logger.Error("send-and-receive-bets", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	return nil
}

func sendAndReceiveBets(conn net.Conn, agencyId string) error {
	const mainAction = "process-bets"
	logger.Info(mainAction, logger.InProgress, "agency-id", agencyId)

	inputPath := os.Getenv("INPUT_FILE")
	outputPath := os.Getenv("OUTPUT_FILE")

	inputFile, err := os.Open(inputPath)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "agency-id", agencyId, "err", err)
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		logger.Error("create-output-file", logger.Fail, "agency-id", agencyId, "err", err)
		return err
	}
	defer outputFile.Close()

	agencyNum, err := strconv.Atoi(agencyId)
	if err != nil {
		logger.Error("parse-agency-id", logger.Fail, "agency-id", agencyId, "err", err)
		return err
	}

	if err := protocol.SendHeader(conn, uint32(agencyNum)); err != nil {
		return err
	}

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		if err := protocol.SendStringMessage(conn, scanner.Text()); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Error("scan-input-file", logger.Fail, "agency-id", agencyId, "err", err)
		return err
	}

	if err := protocol.SendHeader(conn, 0); err != nil {
		return err
	}

	for {
		winnerLine, err := protocol.RecvStringMessage(conn)
		if err != nil {
			return err
		}
		if winnerLine == "" {
			break
		}

		if _, err := outputFile.WriteString(winnerLine + "\n"); err != nil {
			return err
		}
	}

	logger.Info(mainAction, logger.Success, "agency-id", agencyId)
	return nil
}
