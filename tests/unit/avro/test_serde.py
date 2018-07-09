import json
import pytest
import types

from avro.io import DatumReader, SchemaResolutionException
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


BOOK_SCHEMA_WRITE = {
    "namespace": "example.avro",
    "type": "record",
    "name": "Book",
    "fields": [
        {"name": "title", "type": "string"},
        {"name": "first_sentence", "type": "string"},
    ]
}

BOOK_SCHEMA_READ_1 = {
    "namespace": "example.avro",
    "type": "record",
    "name": "Book",
    "fields": [
        {"name": "title", "type": "string"},
        {"name": "first_sentence", "type": "string"},
        {"name": "pages",  "type": ["null", "int"], "default": "null"}
    ]
}

BOOK_SCHEMA_READ_2 = {
    "namespace": "example.avro",
    "type": "record",
    "name": "Book",
    "fields": [
        {"name": "first_sentence", "type": "string"}
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
    (test_meta, test_generator) = deserialize(avro_blob, decode_schema=True)
    assert isinstance(test_meta, dict)
    assert isinstance(test_generator, types.GeneratorType)
    assert isinstance(test_meta['avro.schema'], dict)

    test_schema = test_meta['avro.schema']
    assert test_schema['name'] == USER_SCHEMA['name']
    assert test_schema['namespace'] == USER_SCHEMA['namespace']
    assert test_schema['fields'] == USER_SCHEMA['fields']

    test_records = [value for value in test_generator]
    assert len(test_records) == 1
    assert test_records[0] == user

    # Ensure serialization / deserialization via generators
    # works as expected.
    test_buffer = BytesIO(avro_blob)
    test_meta, test_generator = deserialize(test_buffer)
    assert isinstance(test_meta['avro.schema'], bytes)
    assert isinstance(test_generator, types.GeneratorType)

    # Reform the generator by re-encoding the original, passed
    # as part of a batch.
    test_generator_blob = serialize(USER_SCHEMA, [test_generator])
    _, test_generator = deserialize(test_generator_blob)

    # Ensure the expected data survived.
    test_records = [value for value in test_generator]
    assert len(test_records) == 1
    assert test_records[0] == user


def test_serialize_and_deserialize_with_reader_schema():
    book = {
        'title': 'Nineteen Eighty-Four',
        'first_sentence': 'It was a bright cold day in April, and the clocks were striking thirteen.'
    }

    book_read_1 = {
        'title': 'Nineteen Eighty-Four',
        'first_sentence': 'It was a bright cold day in April, and the clocks were striking thirteen.',
        'pages': None
    }

    book_read_2 = {
        'first_sentence': 'It was a bright cold day in April, and the clocks were striking thirteen.'
    }

    avro_blob = serialize(BOOK_SCHEMA_WRITE, [book])

    (test_meta, test_generator) = deserialize(avro_blob, decode_schema=True, reader_schema=BOOK_SCHEMA_READ_1)
    assert isinstance(test_generator, types.GeneratorType)
    test_records = [value for value in test_generator]
    assert len(test_records) == 1
    assert test_records[0] == book_read_1

    (test_meta, test_generator) = deserialize(avro_blob, decode_schema=True, reader_schema=BOOK_SCHEMA_READ_2)
    assert isinstance(test_generator, types.GeneratorType)
    test_records = [value for value in test_generator]
    assert len(test_records) == 1
    assert test_records[0] == book_read_2

    # test read with incompatible schema
    (test_meta, test_generator) = deserialize(avro_blob, decode_schema=True, reader_schema=USER_SCHEMA)
    assert isinstance(test_generator, types.GeneratorType)
    with pytest.raises(SchemaResolutionException):
        test_records = [value for value in test_generator]


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
