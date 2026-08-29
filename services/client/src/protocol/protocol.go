package protocol

import (
	"io"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

func intToBytes(n uint32) []byte {
	return []byte{
		byte(n >> 24),
		byte(n >> 16),
		byte(n >> 8),
		byte(n),
	}
}

func bytesToInt(b []byte) uint32 {
	return (uint32(b[0]) << 24) |
		(uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) |
		uint32(b[3])
}

func SendHeader(rw io.Writer, value uint32) error {
	return safe_socket.SendAll(rw, intToBytes(value))
}

func SendStringMessage(rw io.Writer, msg string) error {
	msgBytes := []byte(msg)
	header := intToBytes(uint32(len(msgBytes)))

	if err := safe_socket.SendAll(rw, header); err != nil {
		return err
	}
	return safe_socket.SendAll(rw, msgBytes)
}

func SendBatch(rw io.Writer, batch []string) error {
	if len(batch) == 0 {
		return nil
	}
	payload := strings.Join(batch, "\n")
	return SendStringMessage(rw, payload)
}

func RecvStringMessage(r io.Reader) (string, error) {
	header, err := safe_socket.RecvAll(r, 4)
	if err != nil {
		return "", err
	}

	msgLen := bytesToInt(header)
	if msgLen == 0 {
		return "", nil
	}

	payload, err := safe_socket.RecvAll(r, int(msgLen))
	if err != nil {
		return "", err
	}
	return string(payload), nil
}
