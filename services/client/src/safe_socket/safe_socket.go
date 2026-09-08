package safe_socket

import "io"

// SendAll sends all bytes in the provided slice to the given socket. It ensures that all bytes are sent, handling short writes by looping until the entire slice is transmitted or an error occurs.
func SendAll(socket io.Writer, bytes []byte) error {
	sent := 0
	for sent < len(bytes) {
		n, err := socket.Write(bytes[sent:])
		if err != nil {
			return err
		}
		sent += n
	}
	return nil
}

// RecvAll reads all bytes from the given socket until the specified size is reached or an error occurs. It returns the received bytes and any error encountered during the read operation. If the end of the stream is reached before the specified size, it returns the bytes read up to that point.
func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	received := 0
	for received < size {
		n, err := socket.Read(buff[received:])
		if err != nil {
			if err == io.EOF && received > 0 {
				return buff[:received], nil
			}
			return nil, err
		}
		if n == 0 {
			break
		}
		received += n
	}
	return buff[:received], nil
}
