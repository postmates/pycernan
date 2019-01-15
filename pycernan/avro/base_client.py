import random
import struct

from pycernan.avro.client import Client
from pycernan.avro.exceptions import ConnectionResetException, InvalidAckException


def _hash_u64(value):
    return hash(value) % 2 ** 64


def _rand_u64():
    return random.randrange(2 ** 64)


class BaseClient(Client):
    def _send_exact(self, sock, payload):
        sock.sendall(payload)

    def _recv_exact(self, sock, n_bytes):
        buf = bytearray(b'')
        while len(buf) < n_bytes:
            recvd = sock.recv(n_bytes - len(buf))
            if len(recvd) == 0:
                raise ConnectionResetException()

            buf.extend(recvd)

        return bytes(buf)

    def _send(self, payload_id, sync, payload):
        with self.pool.connection() as sock:
            self._send_exact(sock, payload)
            if sync:
                self._wait_for_ack(sock, payload_id)

    def _wait_for_ack(self, sock, payload_id):
        id_bytes = self._recv_exact(sock, 8)
        (recv_id,) = struct.unpack(">Q", id_bytes)
        if recv_id != payload_id:
            raise InvalidAckException()
