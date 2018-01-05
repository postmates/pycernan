

class SchemaParseException(Exception):
    pass


class DatumTypeException(Exception):
    def __init__(self, avro_exception):
        self.avro_exception = avro_exception


class EmptyBatchException(Exception):
    pass


class InvalidAckException(Exception):
    pass
