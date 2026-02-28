#!/usr/bin/env python3
"""
Echo server for the socket extension stress test (stress_test.sp).

TCP: listens on 127.0.0.1:9000 — echoes every byte back to the client.
UDP: listens on 127.0.0.1:9001 — echoes every datagram back to the sender.

Usage:
    python3 stress_server.py
"""

import socket
import threading
import time

TCP_HOST = "127.0.0.1"
TCP_PORT = 9000
UDP_HOST = "127.0.0.1"
UDP_PORT = 9001


# ------------------------------------------------------------------ #
#  Shared stats
# ------------------------------------------------------------------ #
class Stats:
    def __init__(self):
        self._lock        = threading.Lock()
        self.tcp_conns    = 0
        self.tcp_bytes    = 0
        self.udp_datagrams = 0
        self.udp_bytes    = 0

    def add_tcp(self, nbytes):
        with self._lock:
            self.tcp_bytes += nbytes

    def new_tcp_conn(self):
        with self._lock:
            self.tcp_conns += 1
            return self.tcp_conns

    def add_udp(self, nbytes):
        with self._lock:
            self.udp_datagrams += 1
            self.udp_bytes     += nbytes

    def snapshot(self):
        with self._lock:
            return (self.tcp_conns, self.tcp_bytes,
                    self.udp_datagrams, self.udp_bytes)


stats = Stats()


# ------------------------------------------------------------------ #
#  TCP echo handler
# ------------------------------------------------------------------ #
def handle_tcp_client(conn: socket.socket, addr):
    conn_id   = stats.new_tcp_conn()
    conn_bytes = 0
    print(f"[TCP #{conn_id}] connected from {addr}")
    try:
        while True:
            data = conn.recv(65536)
            if not data:
                break
            conn.sendall(data)
            conn_bytes += len(data)
            stats.add_tcp(len(data))
    except Exception as exc:
        print(f"[TCP #{conn_id}] error: {exc}")
    finally:
        conn.close()
        print(f"[TCP #{conn_id}] disconnected — echoed {conn_bytes:,} bytes")


def tcp_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((TCP_HOST, TCP_PORT))
    srv.listen(16)
    print(f"[TCP] listening on {TCP_HOST}:{TCP_PORT}")
    while True:
        try:
            conn, addr = srv.accept()
            threading.Thread(target=handle_tcp_client, args=(conn, addr),
                             daemon=True).start()
        except Exception as exc:
            print(f"[TCP] accept error: {exc}")


# ------------------------------------------------------------------ #
#  UDP echo handler
# ------------------------------------------------------------------ #
def udp_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((UDP_HOST, UDP_PORT))
    print(f"[UDP] listening on {UDP_HOST}:{UDP_PORT}")
    while True:
        try:
            data, addr = srv.recvfrom(65536)
            srv.sendto(data, addr)
            stats.add_udp(len(data))
        except Exception as exc:
            print(f"[UDP] error: {exc}")


# ------------------------------------------------------------------ #
#  Periodic stats printer
# ------------------------------------------------------------------ #
def stats_printer():
    while True:
        time.sleep(5)
        tc, tb, ud, ub = stats.snapshot()
        print(f"[stats] TCP: {tc} connections, {tb:,} bytes echoed  |  "
              f"UDP: {ud:,} datagrams, {ub:,} bytes echoed")


# ------------------------------------------------------------------ #
#  Entry point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    threading.Thread(target=tcp_server,    daemon=True).start()
    threading.Thread(target=udp_server,    daemon=True).start()
    threading.Thread(target=stats_printer, daemon=True).start()

    print("Stress-test echo server running.  Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nServer stopped.")
