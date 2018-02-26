from pycernan.avro.client import Client


class BaseDummyClient(Client):
    def publish_blob(self, avro_blob):
        pass


class DummyClient(BaseDummyClient):
    def _connect(self, host, port):
        pass
