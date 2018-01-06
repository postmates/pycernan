import glob
import json

import mock
import pytest

import pycernan
from pycernan.avro.exceptions import SchemaParseException, DatumTypeException, EmptyBatchException


USER_SCHEMA = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
}


class DummyClient(pycernan.avro.client.Client):
    def _connect(self, host, port):
        pass

    def publish_blob(self, avro_blob):
        pass

@pytest.mark.parametrize("avro_file", glob.glob("./tests/data/*.avro"))
def test_publish_file(avro_file):
    c = DummyClient()
    c.publish_file(avro_file)


def test_publish():
    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }

    c = DummyClient()
    c.publish(USER_SCHEMA, [user])


def test_publish_bad_schema():
    schema = "Not a dict"
    user = {}

    c = DummyClient()
    with pytest.raises(SchemaParseException):
        c.publish(schema, [user])


def test_publish_bad_datum_empty():
    user = {}

    c = DummyClient()
    with pytest.raises(DatumTypeException):
        c.publish(USER_SCHEMA, [user])


def test_publish_empty_batch():
    schema = "Not a dict"

    c = DummyClient()
    with pytest.raises(EmptyBatchException):
        c.publish(USER_SCHEMA, [])
