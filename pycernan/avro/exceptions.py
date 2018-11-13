import avro.io
import avro.schema

SchemaParseException = avro.schema.SchemaParseException
DatumTypeException = avro.io.AvroTypeException


class EmptyBatchException(Exception):
    pass


class InvalidAckException(Exception):
    pass


class ConnectionResetException(Exception):
    pass


class EmptyPoolException(Exception):
    pass
