"""
    V2 Client for Cernan's Avro source.
"""
import struct

from pycernan.avro.base_client import BaseClient, _hash_u64, _rand_u64


class Client(BaseClient):
    """
       V2 of the Avro source protocol.
    """

    VERSION = 2

    def publish_blob(self, avro_blob, sync=True, payload_id=None, shard_by=None, metadata=None):
        """
            Publishes a length prefixed Avro payload to a V2 Avro source.

            Diagram below describes packet layout.

             0                   1                   2                   3
             0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |                             Length                            |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |                            Version                            |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |                            Control                            |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |                                                               |
            +                               ID                              +
            |                                                               |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |                                                               |
            +                            ShardBy                            +
            |                                                               |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |   #KV Pairs   |   Key Length  |    Key (up to 255 bytes)      |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |          Value Length         |   Value (up to 65535 bytes)   |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |                          Avro N Bytes                         |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+


            The fields of the payload have the following semantic meaning:

            * Length  - Length of full packet
            * Version - Version of the wire protocol used.  In this case, 2.
            * Control - Metadata governing on the payload is to be published.
                        Version 1 & 2 of the protocol only supports a bit indicating
                        whether or not the client expects an ack after publication.
            * ID - Explained in kwargs.
            * ShardBy - Explained in kwargs.
            * #KV Pairs - Number of KV pairs (max 255)
            * Key Length - Length of Key in bytes (max 255)
            * Key - N Bytes of Key data encoded as UTF-8 (max 255 bytes after encoding)
            * Value Length - Length of Value data (max 65535)
            * Value - N Bytes of Value data, no encoding enforced (max 65535 bytes)


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
        metadata = metadata if metadata else {}
        kv_encoded = struct.pack(">B", len(metadata))
        for key, val in metadata.items():
            kv_encoded += struct.pack(">B", len(key)) + key.encode("utf-8") + \
                          struct.pack(">H", len(val)) + val.encode("utf-8")
        payload_len = len(header) + len(kv_encoded) + len(avro_blob)
        payload = struct.pack(">L", payload_len) + header + kv_encoded + avro_blob

        self._send(payload_id, sync, payload)
