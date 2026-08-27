package client

import (
	"bufio"
	"net"
	"time"
	"io"
	"os"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
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
	header := intToBytes(uint32(agencyNum))
	if err := safe_socket.SendAll(conn, header); err != nil {
		return err
	}
	
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		lineBytes := []byte(line)
		length := len(lineBytes)
		header := intToBytes(uint32(length))

		if err := safe_socket.SendAll(conn, header); err != nil {
			logger.Error("send-bet-header", logger.Fail, "agency-id", agencyId, "err", err)
			return err
		}
		if err := safe_socket.SendAll(conn, lineBytes); err != nil {
			logger.Error("send-bet-payload", logger.Fail, "agency-id", agencyId, "err", err)
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("scan-input-file", logger.Fail, "agency-id", agencyId, "err", err)
		return err
	}

	endHeader := make([]byte, 4)
	if err := safe_socket.SendAll(conn, endHeader); err != nil {
		return err
	}

	for {
		header, err := safe_socket.RecvAll(conn, 4)
		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Error("recv-winner-header", logger.Fail, "agency-id", agencyId, "err", err)
			return err
		}
		msgLen := bytesToInt(header)
		if msgLen == 0 {
			break
		}

		winnerBuffer, err := safe_socket.RecvAll(conn, int(msgLen))
		if err != nil {
			logger.Error("recv-winner-payload", logger.Fail, "agency-id", agencyId, "err", err)
			return err
		}

		if _, err := outputFile.Write(winnerBuffer); err != nil {
			logger.Error("write-output-file", logger.Fail, "agency-id", agencyId, "err", err)
			return err
		}
		if _, err := outputFile.Write([]byte("\n")); err != nil {
			return err
		}
	}

	logger.Info(mainAction, logger.Success, "agency-id", agencyId)
	return nil
}

func intToBytes(n uint32) []byte {
	b := make([]byte, 4)
	b[0] = byte(n >> 24)
	b[1] = byte(n >> 16)
	b[2] = byte(n >> 8)
	b[3] = byte(n)
	return b
}

func bytesToInt(b []byte) uint32 {
	return (uint32(b[0]) << 24) | 
	       (uint32(b[1]) << 16) | 
	       (uint32(b[2]) << 8)  | 
	       uint32(b[3])
}