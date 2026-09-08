package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
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
		logger.Warn(LOG_CONNECT_TO_SERVER, logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = LOG_CONNECT_TO_SERVER
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

	stopCtxMonitor := make(chan struct{}) // Channel to signal the goroutine to stop monitoring the context
	defer close(stopCtxMonitor)
	go func() {
		select {
		case <-ctx.Done(): // Close connection when signal is received
			client.conn.Close()
		case <-stopCtxMonitor: // defer is executed when Run() returns, so the context monitor goroutine is stopped
			return
		}
	}()

	if err := sendAndReceiveBets(ctx, client.conn, client.config.AgencyId); err != nil {
		if ctx.Err() != nil { // Returns signal error immediately
			return ctx.Err()
		}
		logger.Error(LOG_SEND_AND_RECEIVE_BETS, logger.Fail, ARG_AGENCY_ID, client.config.AgencyId, "err", err)
		return err
	}

	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func sendAndReceiveBets(ctx context.Context, conn net.Conn, agencyId string) error {
	const mainAction = LOG_PROCESS_BETS
	logger.Info(mainAction, logger.InProgress, ARG_AGENCY_ID, agencyId)

	inputPath := os.Getenv("INPUT_FILE")
	outputPath := os.Getenv("OUTPUT_FILE")

	batchSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil {
		logger.Error(LOG_PARSE_BATCH_SIZE, logger.Fail, ARG_AGENCY_ID, agencyId, "err", err)
		return err
	}

	inputFile, err := os.Open(inputPath)
	if err != nil {
		logger.Error(LOG_OPEN_INPUT_FILE, logger.Fail, ARG_AGENCY_ID, agencyId, "err", err)
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		logger.Error(LOG_CREATE_OUTPUT_FILE, logger.Fail, ARG_AGENCY_ID, agencyId, "err", err)
		return err
	}
	defer outputFile.Close()

	agencyNum, err := strconv.Atoi(agencyId)
	if err != nil {
		logger.Error(LOG_PARSE_AGENCY_ID, logger.Fail, ARG_AGENCY_ID, agencyId, "err", err)
		return err
	}

	if err := protocol.SendHeader(conn, uint32(agencyNum)); err != nil {
		return err
	}

	batch := make([]string, 0, batchSize)
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		if ctx.Err() != nil { // Cancels execution when signal is received
			return ctx.Err()
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
		logger.Error(LOG_SCAN_INPUT_FILE, logger.Fail, ARG_AGENCY_ID, agencyId, "err", err)
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
		if ctx.Err() != nil {
			return ctx.Err()
		}

		winnerLine, err := protocol.RecvStringMessage(conn)
		if err != nil {
			return err
		}
		if winnerLine == END_OF_WINNERS_DELIMITER {
			break
		}

		data := winnerLine + "\n"
		n, err := outputFile.WriteString(data)
		if err != nil {
			return err
		}
		if n < len(data) {
			return fmt.Errorf("short write to output file: %d of %d bytes", n, len(data))
		}
	}

	logger.Info(mainAction, logger.Success, ARG_AGENCY_ID, agencyId)
	return nil
}
