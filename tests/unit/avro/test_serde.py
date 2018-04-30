import json
import pytest

from avro.io import DatumReader
from avro.datafile import DataFileReader
from io import BytesIO

from pycernan.avro.exceptions import SchemaParseException, DatumTypeException
from pycernan.avro.serde import parse, serialize, deserialize


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


def test_serialize_bad_schema():
    schema = {}
    user = {}
    with pytest.raises(SchemaParseException):
        serialize(schema, [user])


def test_serialize_bad_datum_empty():
    user = {}
    with pytest.raises(DatumTypeException):
        serialize(USER_SCHEMA, [user])


@pytest.mark.parametrize('schema', [USER_SCHEMA, parse(json.dumps(USER_SCHEMA))])
@pytest.mark.parametrize('ephemeral', [True, False])
def test_serialize(ephemeral, schema):
    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }

    avro_blob = serialize(schema, [user], ephemeral_storage=ephemeral)
    buf = BytesIO()
    buf.write(avro_blob)
    buf.seek(0)
    with DataFileReader(buf, DatumReader()) as reader:
        get_meta = getattr(reader, 'get_meta', None) or reader.GetMeta
        value = get_meta('postmates.storage.ephemeral')
        assert value is (b'1' if ephemeral else None)
        records = [r for r in reader]
        assert records == [user]


def test_serialize_and_deserialize():
    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }

    avro_blob = serialize(USER_SCHEMA, [user])
    (test_meta, test_records) = deserialize(avro_blob, decode_schema=True)
    assert isinstance(test_meta, dict)
    assert isinstance(test_records, list)
    assert isinstance(test_meta['avro.schema'], dict)

    test_schema = test_meta['avro.schema']
    assert test_schema['name'] == USER_SCHEMA['name']
    assert test_schema['namespace'] == USER_SCHEMA['namespace']
    assert test_schema['fields'] == USER_SCHEMA['fields']

    assert len(test_records) == 1
    assert test_records[0] == user

    test_buffer = BytesIO(avro_blob)
    test_meta, test_records2 = deserialize(test_buffer)
    assert isinstance(test_meta['avro.schema'], bytes)
    assert isinstance(test_records2, list)
    assert len(test_records2) == 1
    assert test_records2[0] == user


def test_serialize_with_metadata():
    metadata = {
        'foo.bar': 10,
        'foo.baz': 'foomatic',
    }
    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }

    avro_blob = serialize(USER_SCHEMA, [user], **metadata)
    (test_meta, test_records) = deserialize(avro_blob)

    for k, v, in metadata.items():
        assert test_meta[k] == str(metadata[k]).encode()


def test_deserialize_bad_arg_to_deserialize():
    with pytest.raises(ValueError):
        deserialize(47)
