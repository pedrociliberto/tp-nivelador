import socket
import logger
import safe_socket
from src_frozen.lottery import Lottery, Bet

class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

    def _handle_client(self, client_socket):
        action = "handle-client"
        client_bets = []
        bets_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)

            agency_header = safe_socket.recv_all(client_socket, 4)
            if not agency_header or len(agency_header) < 4:
                return
            agency_id = self._bytes_into_int(agency_header)

            agency_storage_path = f"server_bets_agency_{agency_id}.csv"
            lottery = Lottery(agency_storage_path)
            with open(agency_storage_path, "w"):
                pass

            while True:
                header = safe_socket.recv_all(client_socket, 4)
                if not header:
                    break
                msg_len = self._bytes_into_int(header)
                if msg_len == 0:
                    break

                bet_line_bytes = safe_socket.recv_all(client_socket, msg_len)
                bet_line = bet_line_bytes.decode('utf-8')
                fields = bet_line.split(',')

                bet = Bet(
                    agency_id=agency_id,
                    first_name=fields[0],
                    last_name=fields[1],
                    document=int(fields[2]),
                    birthdate=fields[3],
                    number=int(fields[4])
                )
                client_bets.append(bet)
                bets_amount += 1                

            if client_bets:
                lottery.store_bets(client_bets)
            for stored_bet in lottery.load_bets():
                if lottery.has_won(stored_bet):
                    winner_line = f"{stored_bet.first_name},{stored_bet.last_name},{stored_bet.document},{stored_bet.birthdate},{stored_bet.number}"
                    winner_bytes = winner_line.encode('utf-8')
                    msg_size = len(winner_bytes)
                    winner_header = self._int_to_bytes(msg_size)
                    safe_socket.send_all(client_socket, winner_header + winner_bytes)

            end_header = bytes([0, 0, 0, 0])
            safe_socket.send_all(client_socket, end_header)
            logger.info(action, logger.LogResult.success, "messages-amount", bets_amount)

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", bets_amount
            )
            raise e
        finally:
            client_socket.close()

    def _int_to_bytes(self, n: int) -> bytes:
        b0 = (n >> 24) & 0xFF
        b1 = (n >> 16) & 0xFF
        b2 = (n >> 8) & 0xFF
        b3 = n & 0xFF
        return bytes([b0, b1, b2, b3])

    def _bytes_into_int(self, b: bytes) -> int:
        return (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
