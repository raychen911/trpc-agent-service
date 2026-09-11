"""Test-only TCP fault gate: interrupt this client's connections, never stop a shared DB."""

import select
import socket
import socketserver
import threading


class TcpFaultGate:
    def __init__(self, host, port):
        self.upstream = (host, port)
        self.blocked = threading.Event()
        self.lock = threading.Lock()
        self.connections = set()
        gate = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                if gate.blocked.is_set():
                    return
                try:
                    upstream = socket.create_connection(gate.upstream, timeout=2)
                except OSError:
                    return
                peers = (self.request, upstream)
                with gate.lock:
                    gate.connections.update(peers)
                try:
                    while not gate.blocked.is_set():
                        readable, _, _ = select.select(peers, [], [], 0.05)
                        for peer in readable:
                            data = peer.recv(65536)
                            if not data:
                                return
                            (upstream if peer is self.request else self.request).sendall(data)
                except (OSError, ValueError):
                    pass
                finally:
                    with gate.lock:
                        gate.connections.difference_update(peers)
                    upstream.close()

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

        self.server = Server(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def disconnect(self):
        self.blocked.set()
        with self.lock:
            for connection in tuple(self.connections):
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def reconnect(self):
        self.blocked.clear()

    def __exit__(self, *_):
        self.disconnect()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
