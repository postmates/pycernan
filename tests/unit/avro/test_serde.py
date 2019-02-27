from datetime import datetime
from decimal import Decimal
import pytest
import types

from fastavro import reader
from future.utils import string_types
from io import BytesIO

from pycernan.avro.exceptions import SchemaParseException, SchemaResolutionException, DatumTypeException
from pycernan.avro.serde import parse_schema, serialize, deserialize

def datetime_to_epoch_micros(dt):
    # https://stackoverflow.com/a/8778548
    epoch = datetime(1970, 1, 1)
    td = dt - epoch
    return td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6

TIMESTAMP_MICROS_TYPE = {"type": "long", "logicalType": "timestamp-micros"}
UUID_TYPE = {"type": "string", "logicalType": "uuid"}
MAP_OF_STRINGS_TYPE = {"type": "map", "values": "string"}

EVENT_METADATA_FIELDS = [
    {"name": "event",           "type": "string"},
    {"name": "object_type",     "type": "string"},
    {"name": "object_uuid",     "type": ["long", UUID_TYPE]},
    {"name": "one_stream_uuid", "type": UUID_TYPE},
    {"name": "ts",              "type": TIMESTAMP_MICROS_TYPE},
    {"name": "version",         "type": "int"}
]

SOUND_ENUM = {
    "type": "enum",
    "name": "sound",
    "symbols": [
        "alarm_multi.wav",
        "beep_accept.wav",
        "job_added.wav",
        "job_removed.wav",
        "off_duty_sound.aiff",
        "order_updated.m4a",
    ]
}

EVENT_DATA_RECORD = {
    "type": "record",
    "name": "event_data",
    "fields": [
        {"name": "alert",         "type": "string"},
        {"name": "dispatch_uuid", "type": ["null", UUID_TYPE]},
        {"name": "match_uuid",    "type": ["null", UUID_TYPE]},
        {"name": "message",       "type": "string"},
        {"name": "sound",         "type": ["null", SOUND_ENUM]},
        {"name": "title",         "type": ["null", "string"]},
    ]
}

EVENT_TYPE_ENUM = {
    "type": "enum",
    "name": "event_type",
    "symbols": [
        "account.sync",
        "capabilities.vehicle",
        "courier.force_offduty",
        "courier.force_offduty_silent",
        "courier_dispatch.canceled",
        "courier_dispatch.new",
        "job.admin_canceled",
        "job.assigned_from",
        "job.assigned_to",
        "job.auto_assigned",
        "job.customer_canceled",
        "job.issue.updated",
        "job.last_job",
        "job.order.updated",
        "job.pex_funds_added",
        "job.pex_funds_not_added",
        "job.reversed_to",
        "message",
        "verify.id",
        "verify.id_and_over_21",
        "verify.id_and_over_21_and_signature",
        "verify.id_and_over_18",
        "verify.id_and_over_18_and_signature",
        "verify.id_and_signature",
    ]
}


DATA_1_RECORD = {
    "type": "record",
    "name": "data_1",
    "fields": [
        {"name": "event_data",     "type": EVENT_DATA_RECORD},
        {"name": "event_need_ack", "type": "boolean"},
        {"name": "event_type",     "type": ["null", EVENT_TYPE_ENUM]},
        {"name": "event_uuid",     "type": UUID_TYPE},
    ]
}

DATA_2_RECORD = {
    "type": "record",
    "name": "data_2",
    "fields": [
        {"name": "message_data",         "type": MAP_OF_STRINGS_TYPE},  # todo: unconfirmed
        {"name": "message_fetch_method", "type": "string"},  # todo: enum
        {"name": "message_kind",         "type": "string"},  # todo: enum
        {"name": "message_needs_ack",    "type": "boolean"},
        {"name": "message_uuid",         "type": UUID_TYPE},
    ]
}

# todo: event_created events seem to have two totally different signatures
COURIER_EVENT_CREATED_EVENT_RECORD = {
    "type": "record",
    "namespace": "public.courier",
    "name": "event_created",
    "fields": [
        {"name": "courier_uuid", "type": UUID_TYPE},
        {"name": "data",         "type": [DATA_1_RECORD, DATA_2_RECORD]},
    ] + EVENT_METADATA_FIELDS
}

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
        {"name": "pages",  "type": ["null", "int"], "default": None}
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
    schema = {
        # Name is missing
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number",  "type": ["int", "null"]},
            {"name": "favorite_color", "type": ["string", "null"]}
        ]
    }
    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }
    with pytest.raises(SchemaParseException):
        serialize(schema, [user])


def test_serialize_bad_datum_empty():
    user = {}
    with pytest.raises(DatumTypeException):
        serialize(USER_SCHEMA, [user])


@pytest.mark.parametrize('schema', [USER_SCHEMA, parse_schema(USER_SCHEMA)])
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

    read = reader(buf)
    meta = read.metadata
    value = meta.get('postmates.storage.ephemeral', None)
    assert value == ('1' if ephemeral else None)
    records = [r for r in read]
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
    assert test_schema['name'] == '.'.join([USER_SCHEMA['namespace'], USER_SCHEMA['name']])
    assert test_schema['fields'] == USER_SCHEMA['fields']

    test_records = [value for value in test_generator]
    assert len(test_records) == 1
    assert test_records[0] == user

    # Ensure serialization / deserialization via generators
    # works as expected.
    test_buffer = BytesIO(avro_blob)
    test_meta, test_generator = deserialize(test_buffer)
    assert isinstance(test_meta['avro.schema'], string_types)
    assert isinstance(test_generator, types.GeneratorType)

    # Reform the generator by re-encoding the original, passed
    # as part of a batch.
    test_generator_blob = serialize(USER_SCHEMA, [test_generator])
    _, test_generator = deserialize(test_generator_blob)

    # Ensure the expected data survived.
    test_records = [value for value in test_generator]
    assert len(test_records) == 1
    assert test_records[0] == user


TEST_SCHEMA_LOGICAL_TYPES = {
    "type": "record",
    "namespace": "com.postmates.test",
    "name": "test_event_logical_types",
    "fields": [
        {
            "name": "ts",
            "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}],
            "default": None,
            "doc": "some timestamp",
        },
        {
            "name": "customer_uuid",
            "type": ["null", "string"],
            "default": None,
            "doc": "customer uuid",
        },
        {
            "name": "decimal_bytes",
            "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 3}],
            "default": None,
            "doc": "some decimal",
        },
        {
            "name": "decimal_fixed",
            "type": ["null", {"name": "n", "type": "fixed", "size": 8, "logicalType": "decimal", "precision": 15, "scale": 3}],
            "default": None,
            "doc": "some decimal",
        },
    ]
}


def test_logical_types():
    from datetime import datetime
    from pytz import timezone

    event = {
        'ts': datetime.utcnow().replace(tzinfo=timezone('UTC')),
        'customer_uuid': 'some_random_uuid',
        'decimal_bytes': Decimal("-2.90"),
        'decimal_fixed': Decimal("3.68"),
    }

    avro_blob = serialize(TEST_SCHEMA_LOGICAL_TYPES, [event])

    (test_meta, test_generator) = deserialize(avro_blob, decode_schema=True)
    assert isinstance(test_generator, types.GeneratorType)
    test_records = [value for value in test_generator]
    assert len(test_records) == 1
    assert test_records[0] == event


def test_serde_malformed_input():
    sample_courier_event_created_payload_1 = {
        'courier_uuid': '0d6ce262-086c-4c15-90f9-a9448a696f8d',
        'data': {
            'event_data': {
                'alert': 'Delivery Complete! You earned $4.77 before tip.',
                'message': 'Delivery Complete! You earned $4.77 before tip.',
                'sound': None,
                'title': None
            },
            'event_need_ack': False,
            'event_type': 'message',
            'event_uuid': '81832ca5-5e53-4096-8868-58c41dbede79',
        },
        'event': 'event_created',
        'object_type': 'courier',
        'object_uuid': '0d6ce262-086c-4c15-90f9-a9448a696f8d',
        'ts': datetime_to_epoch_micros(datetime.utcnow()),
        'version': 2,
        'one_stream_uuid': '09963f9f06fb477797f88f8b09530364'
    }
    serialize(COURIER_EVENT_CREATED_EVENT_RECORD, [sample_courier_event_created_payload_1])



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
        assert test_meta[k] == str(metadata[k])


def test_deserialize_bad_arg_to_deserialize():
    with pytest.raises(ValueError):
        deserialize(47)
