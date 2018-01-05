import glob
import json
import mock
import pytest
import pycernan.avro.v1

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


@pytest.mark.parametrize("avro_file", glob.glob("./tests/data/*.avro"))
@mock.patch('pycernan.avro.v1.Client._connect', return_value=None)
@mock.patch('pycernan.avro.v1.Client.publish_blob', return_value=None)
def test_publish_file(publish_blob_mock, connect_mock, avro_file):
    c = pycernan.avro.v1.Client()
    c.publish_file(avro_file)


@mock.patch('pycernan.avro.v1.Client._connect', return_value=None)
@mock.patch('pycernan.avro.v1.Client.publish_blob', return_value=None)
def test_publish(_publish_bloc_mock, _connect_mock):
    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }

    c = pycernan.avro.v1.Client()
    c.publish(USER_SCHEMA, [user])


@mock.patch('pycernan.avro.v1.Client._connect', return_value=None)
def test_publish_bad_schema(_connect_mock):
    schema = "Not a dict"
    user = {}

    c = pycernan.avro.v1.Client()
    with pytest.raises(SchemaParseException):
        c.publish(schema, [user])


@mock.patch('pycernan.avro.v1.Client._connect', return_value=None)
def test_publish_bad_datum_empty(_connect_mock):
    user = {}

    c = pycernan.avro.v1.Client()
    with pytest.raises(DatumTypeException):
        c.publish(USER_SCHEMA, [user])


@mock.patch('pycernan.avro.v1.Client._connect', return_value=None)
def test_publish_empty_batch(_connect_mock):
    schema = "Not a dict"

    c = pycernan.avro.v1.Client()
    with pytest.raises(EmptyBatchException):
        c.publish(USER_SCHEMA, [])
