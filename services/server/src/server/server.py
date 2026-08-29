import socket
import logger
import protocol
from formatting import parse_bet, format_winner
from lottery import Lottery, Bet

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

            agency_id = protocol.recv_header(client_socket)
            if agency_id is None:
                return

            agency_storage_path = f"server_bets_agency_{agency_id}.csv"
            lottery = Lottery(agency_storage_path)
            with open(agency_storage_path, "w"):
                pass

            while True:
                bet_lines = protocol.recv_batch(client_socket)
                if not bet_lines:
                    break
                for line in bet_lines:
                    if line:
                        client_bets.append(parse_bet(agency_id, line))
                        bets_amount += 1              

            if client_bets:
                lottery.store_bets(client_bets)
            for stored_bet in lottery.load_bets():
                if lottery.has_won(stored_bet):
                    winner_line = format_winner(stored_bet)
                    protocol.send_string_message(client_socket, winner_line)

            protocol.send_header(client_socket, 0)
            logger.info(action, logger.LogResult.success, "messages-amount", bets_amount)

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", bets_amount
            )
            raise e
        finally:
            client_socket.close()

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
