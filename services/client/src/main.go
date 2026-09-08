package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

// Loads the client configuration from environment variables. It checks for the presence of required variables and returns an error if any are missing. If all required variables are present, it constructs and returns a ClientConfig struct.
func loadConfig() (client.ClientConfig, error) {
	agencyId := os.Getenv("AGENCY_ID")
	if agencyId == "" {
		return client.ClientConfig{}, errors.New("AGENCY_ID environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   agencyId,
	}, nil
}

// Entry point of the client application. It initializes the client, sets up signal handling for graceful shutdown, and starts the client's main run loop.
func main() {
	// Creates context that is cancelled when signal is received
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return
	}

	client, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		return
	}

	go func() {
		<-ctx.Done() // Blocks here until shutdown signal is received
		logger.Info("signal-received", logger.InProgress, "msg", "Graceful shutdown initiated")
		client.Close()
	}()

	if err := client.Run(ctx); err != nil {
		if errors.Is(ctx.Err(), context.Canceled) { // Context explicitly cancelled by SIGTERM: graceful
			logger.Info("client-shutdown", logger.Success)
			return
		}
		logger.Error("client-run", logger.Fail, "err", err)
		return
	}
}
