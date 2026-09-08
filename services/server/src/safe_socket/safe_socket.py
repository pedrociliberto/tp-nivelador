import socket

def send_all(socket: socket.socket, bytes: bytes):
    """
    Send all bytes to the socket, handling short writes.
    """
    sent = 0
    while sent < len(bytes):
        n = socket.send(bytes[sent:])
        sent += n

def recv_all(socket: socket.socket, size: int) -> bytes:
    """
    Receive exactly `size` bytes from the socket, handling short reads.
    """
    buffer = bytearray()
    while len(buffer) < size:
        received = socket.recv(size - len(buffer))
        if not received:
            break
        buffer.extend(received)
    return bytes(buffer)
