"""
    V1 Client for Cernan's Avro source.
"""
import struct

from pycernan.avro.base_client import BaseClient, _hash_u64, _rand_u64

class Client(BaseClient):
    """
       V1 of the Avro source protocol.
    """

    VERSION = 1

    def publish_blob(self, avro_blob, sync=True, payload_id=None, shard_by=None):
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
                sync : bool - Wait for acknowledgment that the payload has been published?  Default = True.
                id : int - Optional identifier for the payload.
                shard_by : hashable value - Used to allocate the payload into a downstream bucket
                           (order is only preserved between entries allocated to the same bucket).
        """
        version = self.VERSION
        sync = 1 if sync else 0
        payload_id = int(payload_id) if payload_id else _rand_u64()
        shard_by = _hash_u64(shard_by) if shard_by else _rand_u64()
        header = struct.pack(">LLQQ", version, sync, payload_id, shard_by)
        payload_len = len(header) + len(avro_blob)
        payload = struct.pack(">L", payload_len) + header + avro_blob

        self._send(payload_id, sync, payload)
