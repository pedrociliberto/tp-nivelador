import socket
import os
import signal
import threading
import logger
import protocol
from . import formatting
from lottery import Lottery, Bet

class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.running = True
        self.server_socket = None
        self.active_threads = []
        self.active_clients = []
        self.clients_lock = threading.Lock()

        self.quorum_min = int(os.getenv("AGENCY_QUORUM_MIN"))
        self.quorum_barrier = threading.Barrier(self.quorum_min)
        self.storage_path = "server_bets.csv"
        self.lottery = Lottery(self.storage_path)
        self.lottery_lock = threading.Lock()

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        with open(self.storage_path, "w"):
            pass

    def _handle_signal(self, signum, frame):
        logger.info("signal-received", logger.LogResult.in_progress, f"Signal {signum} received")
        self.running = False

        try: # Forces BrokenBarrierError to free waiting threads
            self.quorum_barrier.abort()
        except Exception:
            pass

        if self.server_socket: # Closes main socket
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.server_socket.close()
            except Exception:
                pass

        with self.clients_lock: # Closes active threads sockets to free blocking batch recvs
            for client_socket in self.active_clients:
                try:
                    client_socket.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    client_socket.close()
                except Exception:
                    pass

    def _register_client_socket(self, sock):
        with self.clients_lock:
            self.active_clients.append(sock)

    def _unregister_client_socket(self, sock):
        with self.clients_lock:
            if sock in self.active_clients:
                self.active_clients.remove(sock)

    def _handle_client(self, client_socket):
        action = "handle-client"
        client_bets = []
        bets_amount = 0
        self._register_client_socket(client_socket) # Trace socket in case of SIGTERM
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

            if not self.running:
                return

            try:
                self.quorum_barrier.wait()
            except threading.BrokenBarrierError:
                return

            logger.info("quorum-wait", logger.LogResult.success, "agency-id", agency_id)

            bets_generator = self.lottery.load_bets()
            try:
                for stored_bet in bets_generator:
                    if not self.running:
                        break
                    if stored_bet.agency_id == agency_id and self.lottery.has_won(stored_bet):
                        winner_line = formatting.format_winner(stored_bet)
                        protocol.send_string_message(client_socket, winner_line)
            finally: # This block ensures file closure
                bets_generator.close()

            if self.running:
                protocol.send_header(client_socket, 0)
                logger.info(action, logger.LogResult.success, "messages-amount", bets_amount)

        except Exception as e:
            if self.running:
                logger.error(
                    action, logger.LogResult.fail, "messages-amount", bets_amount
                )
                raise e
        finally:
            self._unregister_client_socket(client_socket)
            try:
                client_socket.close()
            except Exception:
                pass

    def run(self):
        action = "accept-connection"
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Re-use port on rapid restart

        try: 
            self.server_socket.bind((self.server_host, self.server_port))
            self.server_socket.listen()
        
            while self.running:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = self.server_socket.accept()
                    logger.info(action, logger.LogResult.success)

                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket,)
                    )
                    client_thread.start()
                    self.active_threads.append(client_thread)
                except (OSError, socket.error):
                    # When socket is closed from _handle_signal
                    break
        finally:
            self.running = False
            if self.server_socket:
                try:
                    self.server_socket.close()
                except Exception:
                    pass
            for t in self.active_threads:
                if t.is_alive():
                    t.join(timeout=1.0)
            logger.info("server-shutdown", logger.LogResult.success)