import socket
import safe_socket

def _int_to_bytes(n: int) -> bytes:
    return bytes([
        (n >> 24) & 0xFF,
        (n >> 16) & 0xFF,
        (n >> 8) & 0xFF,
        n & 0xFF
    ])

def _bytes_to_int(b: bytes) -> int:
    return (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]

def send_header(sock: socket.socket, value: int):
    safe_socket.send_all(sock, _int_to_bytes(value))

def send_string_message(sock: socket.socket, msg: str):
    msg_bytes = msg.encode('utf-8')
    header = _int_to_bytes(len(msg_bytes))
    safe_socket.send_all(sock, header + msg_bytes)

def recv_header(sock: socket.socket) -> int | None:
    header = safe_socket.recv_all(sock, 4)
    if not header or len(header) < 4:
        return None
    return _bytes_to_int(header)

def recv_string_message(sock: socket.socket) -> str | None:
    msg_len = recv_header(sock)
    if msg_len is None or msg_len == 0:
        return None
    
    payload = safe_socket.recv_all(sock, msg_len)
    return payload.decode('utf-8')