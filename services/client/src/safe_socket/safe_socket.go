package safe_socket

import "io"

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
	return buff, nil
}
