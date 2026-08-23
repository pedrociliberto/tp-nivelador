package client

import (
	"bufio"
	"net"
	"time"
	"os"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const ECHO_CLIENT_BUFFER_SIZE = 512
const ECHO_CLIENT_MESSAGE_AMOUNT = 3
const ECHO_CLIENT_MESSAGE_DELAY_MS = 1000

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
	const mainAction = "test-echo-server"
	defer client.conn.Close()

	for messageId := range ECHO_CLIENT_MESSAGE_AMOUNT {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		clientMessage := client.config.AgencyId

		if err := safe_socket.SendAll(client.conn, []byte(clientMessage)); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}

		responseBuffer, err := safe_socket.RecvAll(client.conn, ECHO_CLIENT_BUFFER_SIZE)
		if err != nil {
			logger.Error("recv-response", logger.Fail, messageArgs...)
			return err
		}

		if string(responseBuffer) != clientMessage {
			logger.Error("check-response", logger.Fail, messageArgs...)
			return err
		}

		time.Sleep(ECHO_CLIENT_MESSAGE_DELAY_MS * time.Millisecond)
	}
	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

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

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()

		if err := safe_socket.SendAll(conn, []byte(line)); err != nil {
			logger.Error("send-bet", logger.Fail, "agency-id", agencyId, "line", line, "err", err)
			return err
		}

		responseBuffer, err := safe_socket.RecvAll(conn, ECHO_CLIENT_BUFFER_SIZE)
		if err != nil {
			logger.Error("recv-response", logger.Fail, "agency-id", agencyId, "err", err)
			return err
		}

		if _, err := outputFile.Write(responseBuffer); err != nil {
			logger.Error("write-output-file", logger.Fail, "agency-id", agencyId, "err", err)
			return err
		}
		if _, err := outputFile.Write([]byte("\n")); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("scan-input-file", logger.Fail, "agency-id", agencyId, "err", err)
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", agencyId)
	return nil
}
