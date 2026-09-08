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

// intToBytes converts a uint32 integer to a byte slice of length 4, representing the integer in big-endian order.
func intToBytes(n uint32) []byte {
	return []byte{
		byte(n >> SHIFT_BYTE_3),
		byte(n >> SHIFT_BYTE_2),
		byte(n >> SHIFT_BYTE_1),
		byte(n),
	}
}

// bytesToInt converts a byte slice of length 4 to a uint32 integer, interpreting the bytes in big-endian order.
func bytesToInt(b []byte) uint32 {
	return (uint32(b[0]) << SHIFT_BYTE_3) |
		(uint32(b[1]) << SHIFT_BYTE_2) |
		(uint32(b[2]) << SHIFT_BYTE_1) |
		uint32(b[3])
}

// SendHeader sends a 4-byte header containing the length of the message to the provided writer. It uses the intToBytes function to convert the uint32 value to a byte slice before sending it.
func SendHeader(rw io.Writer, value uint32) error {
	return safe_socket.SendAll(rw, intToBytes(value))
}

// SendStringMessage sends a string message over the provided writer. It first sends a header containing the length of the message, followed by the message itself.
func SendStringMessage(rw io.Writer, msg string) error {
	msgBytes := []byte(msg)
	msgLen := uint32(len(msgBytes))

	packet := make([]byte, HEADER_SIZE_BYTES+msgLen)
	copy(packet[0:HEADER_SIZE_BYTES], intToBytes(msgLen))
	copy(packet[HEADER_SIZE_BYTES:], msgBytes)

	return safe_socket.SendAll(rw, packet)
}

// SendBatch sends a batch of string messages over the provided writer. It concatenates the messages into a single string, separated by newlines, and sends it using the SendStringMessage function.
func SendBatch(rw io.Writer, batch []string) error {
	if len(batch) == 0 {
		return nil
	}
	payload := strings.Join(batch, "\n")
	return SendStringMessage(rw, payload)
}

// RecvStringMessage receives a string message from the provided reader. It first reads a 4-byte header to determine the length of the incoming message, then reads the message itself and returns it as a string.
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

// RecvACK reads a 4-byte acknowledgment from the provided reader. It checks if the received value matches the expected ACK_SUCCESS_VALUE and returns an error if it doesn't.
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
