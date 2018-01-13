"""
    V1 Client for Cernan's Avro source.
"""
import random
import struct

from pycernan.avro import client
from pycernan.avro.exceptions import InvalidAckException, ConnectionResetException


def _hash_u64(value):
    return hash(value) % 2**64


def _rand_u64():
    return random.randrange(2**64)


class Client(client.Client):
    """
       V1 of the Avro source protocol.
    """

    VERSION = 1

    def publish_blob(self, avro_blob, id=None, shard_by=None):
        """
            Publishes an length prefixed avro payload to a V1 Avro source.

            The following describes the avro payload which follows a 4 byte, big-endian length:

                    | version - 4 bytes   |   control - 4 bytes  |
                    |               id - 8 bytes                 |
                    |           shard_by - 8 bytes               |
                    |               avro - N bytes               |


            The fields of the payload have the following semantic meaning:

            * version - Version of the wire protocol used.  In this case, 1.
            * control - Metadata governing on the payload is to be published.
                        Version 1 of the protocol only supports a bit indicating
                        whether or not the client expects an ack after publication.
            * id - Explained in kwargs.
            * shard_by - Explained in kwargs.


            Kwargs:
                id : int - Optional identifier for the payload.  If not None, then the publish
                           will be treated as synchronous.  Blocking until Cernan ACKS the id.
                shard_by : hashable value - Used to allocate the payload into a downstream bucket
                           (order is only preserved between entries allocated to the same bucket).
        """
        version = self.VERSION
        sync = 1 if id else 0
        id = int(id) if id else _rand_u64()
        shard_by = _hash_u64(shard_by) if shard_by else _rand_u64()
        header = struct.pack(">LLQQ", version, sync, id, shard_by)
        payload_len = len(header) + len(avro_blob)
        payload = struct.pack(">L", payload_len) + header + avro_blob

        self._send(id, sync, payload)

    def _send(self, id, sync, payload):
        self._send_exact(payload)
        if sync:
            self._wait_for_ack(id)

    def _wait_for_ack(self, id):
        id_bytes = self._recv_exact(8)
        (recv_id,) = struct.unpack(">Q", id_bytes)

        if recv_id != id:
            raise InvalidAckException()

    def _send_exact(self, payload):
        self.sock.sendall(payload)

    def _recv_exact(self, n_bytes):
        buf = bytearray(b'')
        while len(buf) < n_bytes:
            recvd = self.sock.recv(n_bytes - len(buf))
            if len(recvd) == 0:
                raise ConnectionResetException()

            buf.extend(recvd)

        return bytes(buf)
