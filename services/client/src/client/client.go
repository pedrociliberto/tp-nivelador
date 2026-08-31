package client

import (
	"bufio"
	"context"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const (
	CONNECTION_ATTEMPTS_MAX     = 3
	CONNECTION_ATTEMPS_DELAY_MS = 200
	END_OF_BETS_HEADER_ID       = 0
	END_OF_WINNERS_DELIMITER    = ""
)

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

func (client *Client) Run(ctx context.Context) error {
	defer client.conn.Close()

	if err := sendAndReceiveBets(ctx, client.conn, client.config.AgencyId); err != nil {
		if ctx.Err() != nil { // Returns signal error immediately
			return ctx.Err()
		}
		logger.Error("send-and-receive-bets", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func sendAndReceiveBets(ctx context.Context, conn net.Conn, agencyId string) error {
	const mainAction = "process-bets"
	logger.Info(mainAction, logger.InProgress, "agency-id", agencyId)

	inputPath := os.Getenv("INPUT_FILE")
	outputPath := os.Getenv("OUTPUT_FILE")

	batchSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil {
		logger.Error("parse-batch-size", logger.Fail, "agency-id", agencyId, "err", err)
		return err
	}

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

	batch := make([]string, 0, batchSize)
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		select { // Cancels execution when signal is received
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()
		batch = append(batch, line)

		if len(batch) == batchSize {
			if err := protocol.SendBatch(conn, batch); err != nil {
				return err
			}
			if err := protocol.RecvACK(conn); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Error("scan-input-file", logger.Fail, "agency-id", agencyId, "err", err)
		return err
	}

	if len(batch) > 0 {
		if err := protocol.SendBatch(conn, batch); err != nil {
			return err
		}
		if err := protocol.RecvACK(conn); err != nil {
			return err
		}
	}

	if err := protocol.SendHeader(conn, END_OF_BETS_HEADER_ID); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		winnerLine, err := protocol.RecvStringMessage(conn)
		if err != nil {
			return err
		}
		if winnerLine == END_OF_WINNERS_DELIMITER {
			break
		}

		if _, err := outputFile.WriteString(winnerLine + "\n"); err != nil {
			return err
		}
	}

	logger.Info(mainAction, logger.Success, "agency-id", agencyId)
	return nil
}
