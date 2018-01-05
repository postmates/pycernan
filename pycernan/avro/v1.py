"""
    V1 Client for Cernan's Avro source.
"""

import struct
import uuid

from pycernan.avro import client
from pycernan.avro.exceptions import InvalidAckException

_VERSION = 1


def _rand_u64():
    return uuid.uuid1().int>>64


class Client(client.Client):
    """
       V1 of the Avro source protocol.
    """

    def publish_blob(self, avro_blob, id=None, order_by=None):
        """
            Publishes an avro payload to a V1 Avro source.

            The following payload is generated and placed onto the established tcp connection:

                    | version - 4 bytes   |   control - 4 bytes  |
                    |               id - 8 bytes                 |
                    |           order_by - 8 bytes               |
                    |               avro - N bytes               |

            Kwargs:
                id : int - Optional identifier for the payload.  If not None, then the publish
                           will be treated as synchronous.  Blocking until Cernan ACKS the id.

                order_by : int - Value used to order the event in downstream buckets.
        """
        version = _VERSION
        sync = 1 if id else 0
        id = int(id) if id else _rand_u64()
        order_by = order_by or _rand_u64()
        header = struct.pack(">LLQQ", version, sync, id, order_by)
        payload_len = header.__len__() + avro_blob.__len__()
        payload = struct.pack(">L", payload_len) + header + avro_blob

        self.sock.send(payload)

        if sync:
            self.wait_for_ack(id)

        def wait_for_ack(self, id):
            id_bytes = self.sock.recv(4)
            recv_id = int.from_bytes(id_bytes, byteorder='big')

            if recv_id != id:
                raise InvalidAckException()
