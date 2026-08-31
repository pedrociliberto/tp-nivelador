package protocol

import (
	"io"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	ACK_SUCCESS_VALUE = 1
	HEADER_SIZE_BYTES = 4

	SHIFT_BYTE_3 = 24
	SHIFT_BYTE_2 = 16
	SHIFT_BYTE_1 = 8
)

func intToBytes(n uint32) []byte {
	return []byte{
		byte(n >> SHIFT_BYTE_3),
		byte(n >> SHIFT_BYTE_2),
		byte(n >> SHIFT_BYTE_1),
		byte(n),
	}
}

func bytesToInt(b []byte) uint32 {
	return (uint32(b[0]) << SHIFT_BYTE_3) |
		(uint32(b[1]) << SHIFT_BYTE_2) |
		(uint32(b[2]) << SHIFT_BYTE_1) |
		uint32(b[3])
}

func SendHeader(rw io.Writer, value uint32) error {
	return safe_socket.SendAll(rw, intToBytes(value))
}

func SendStringMessage(rw io.Writer, msg string) error {
	msgBytes := []byte(msg)
	msgLen := uint32(len(msgBytes))

	packet := make([]byte, HEADER_SIZE_BYTES+msgLen)
	copy(packet[0:HEADER_SIZE_BYTES], intToBytes(msgLen))
	copy(packet[HEADER_SIZE_BYTES:], msgBytes)

	return safe_socket.SendAll(rw, packet)
}

func SendBatch(rw io.Writer, batch []string) error {
	if len(batch) == 0 {
		return nil
	}
	payload := strings.Join(batch, "\n")
	return SendStringMessage(rw, payload)
}

func RecvStringMessage(r io.Reader) (string, error) {
	header, err := safe_socket.RecvAll(r, HEADER_SIZE_BYTES)
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

func RecvACK(r io.Reader) error {
	header, err := safe_socket.RecvAll(r, HEADER_SIZE_BYTES)
	if err != nil {
		return err
	}
	if bytesToInt(header) != ACK_SUCCESS_VALUE {
		return io.ErrUnexpectedEOF
	}
	return nil
}
