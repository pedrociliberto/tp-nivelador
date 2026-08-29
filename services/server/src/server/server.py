import socket
import os
import threading
import logger
import protocol
from . import formatting
from lottery import Lottery, Bet

class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.quorum_min = int(os.getenv("AGENCY_QUORUM_MIN"))
        self.quorum_barrier = threading.Barrier(self.quorum_min)
        self.storage_path = "server_bets.csv"
        self.lottery = Lottery(self.storage_path)
        self.lottery_lock = threading.Lock()

        with open(self.storage_path, "w"):
            pass

    def _handle_client(self, client_socket):
        action = "handle-client"
        client_bets = []
        bets_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)

            agency_id = protocol.recv_header(client_socket)
            if agency_id is None:
                return

            while True:
                bet_lines = protocol.recv_batch(client_socket)
                if not bet_lines:
                    break
                for line in bet_lines:
                    if line:
                        client_bets.append(formatting.parse_bet(agency_id, line))
                        bets_amount += 1          
                protocol.send_ack(client_socket)

            if client_bets:
                with self.lottery_lock:
                    self.lottery.store_bets(client_bets)

            logger.info("quorum-wait", logger.LogResult.in_progress, "agency-id", agency_id)
            self.quorum_barrier.wait()
            logger.info("quorum-wait", logger.LogResult.success, "agency-id", agency_id)

            for stored_bet in self.lottery.load_bets():
                if stored_bet.agency_id == agency_id and self.lottery.has_won(stored_bet):
                    winner_line = formatting.format_winner(stored_bet)
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
                    logger.info(action, logger.LogResult.success)

                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket,)
                    )
                    client_thread.start()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e