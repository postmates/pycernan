import fastavro

SchemaParseException = (fastavro.schema.SchemaParseException, KeyError)
SchemaResolutionException = fastavro.read.SchemaResolutionError
UnknownTypeException = fastavro.schema.UnknownType
DatumTypeException = ValueError


class EmptyBatchException(Exception):
    pass


class InvalidAckException(Exception):
    pass


class ConnectionResetException(Exception):
    pass


class EmptyPoolException(Exception):
    pass
