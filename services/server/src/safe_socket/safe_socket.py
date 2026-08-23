import socket

def send_all(socket: socket.socket, bytes: bytes):
    sent = 0
    while sent < len(bytes):
        n = socket.send(bytes[sent:])
        if n == 0:
            raise RuntimeError("socket connection broken")        
        sent += n

def recv_all(socket: socket.socket, size: int) -> bytes:
    buffer = bytearray()
    while len(buffer) < size:
        received = socket.recv(size - len(buffer))
        if not received:
            break
        buffer.extend(received)
    return bytes(buffer)
