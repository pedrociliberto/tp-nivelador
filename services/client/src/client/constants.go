package client

const (
	CONNECTION_ATTEMPTS_MAX     = 3
	CONNECTION_ATTEMPS_DELAY_MS = 200
	END_OF_BETS_HEADER_ID       = 0
	END_OF_WINNERS_DELIMITER    = ""

	ARG_AGENCY_ID             = "agency-id"
	LOG_CONNECT_TO_SERVER     = "connect-to-server"
	LOG_SEND_AND_RECEIVE_BETS = "send-and-receive-bets"
	LOG_PROCESS_BETS          = "process-bets"
	LOG_PARSE_BATCH_SIZE      = "parse-batch-size"
	LOG_OPEN_INPUT_FILE       = "open-input-file"
	LOG_SCAN_INPUT_FILE       = "scan-input-file"
	LOG_CREATE_OUTPUT_FILE    = "create-output-file"
	LOG_PARSE_AGENCY_ID       = "parse-agency-id"
)
