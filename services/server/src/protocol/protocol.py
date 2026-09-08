import socket
import safe_socket

ACK_CODE = 1
SHIFT_FIRST_BYTE = 24
SHIFT_SECOND_BYTE = 16
SHIFT_THIRD_BYTE = 8

def _int_to_bytes(n: int) -> bytes:
    """
    Convert an integer to a 4-byte big-endian representation.
    """
    return bytes([
        (n >> SHIFT_FIRST_BYTE) & 0xFF,
        (n >> SHIFT_SECOND_BYTE) & 0xFF,
        (n >> SHIFT_THIRD_BYTE) & 0xFF,
        n & 0xFF
    ])

def _bytes_to_int(b: bytes) -> int:
    """
    Convert a 4-byte big-endian representation to an integer.
    """
    return (b[0] << SHIFT_FIRST_BYTE) | (b[1] << SHIFT_SECOND_BYTE) | (b[2] << SHIFT_THIRD_BYTE) | b[3]

def send_header(sock: socket.socket, value: int):
    """
    Send a 4-byte header representing the integer `value` to the socket.
    """
    safe_socket.send_all(sock, _int_to_bytes(value))

def send_string_message(sock: socket.socket, msg: str):
    """
    Send a string message to the socket, prefixed with its length as a 4-byte header.
    """
    msg_bytes = msg.encode('utf-8')
    header = _int_to_bytes(len(msg_bytes))
    safe_socket.send_all(sock, header + msg_bytes)

def recv_header(sock: socket.socket) -> int | None:
    """
    Receive a 4-byte header from the socket and convert it to an integer.
    Returns `None` if the header could not be read.
    """
    header = safe_socket.recv_all(sock, 4)
    if not header or len(header) < 4:
        return None
    return _bytes_to_int(header)

def recv_string_message(sock: socket.socket) -> str | None:
    """
    Receive a string message from the socket, which is prefixed with its length as a 4-byte header.
    Returns `None` if the message could not be read.
    """
    msg_len = recv_header(sock)
    if msg_len is None or msg_len == 0:
        return None
    
    payload = safe_socket.recv_all(sock, msg_len)
    return payload.decode('utf-8')

def recv_batch(sock: socket.socket) -> list[str] | None:
    """
    Receive a batch of string messages from the socket, where the batch is represented as a single string with messages separated by newlines.
    Returns `None` if the batch could not be read.
    """
    payload = recv_string_message(sock)
    if not payload:
        return None
    return [line for line in payload.split('\n') if line]

def send_ack(sock: socket.socket):
    """
    Send an acknowledgment to the socket.
    """
    send_header(sock, ACK_CODE)