import os

import pycernan.config


def host():
    return os.getenv("PYCERNAN_AVRO_HOST", None) or pycernan.config.host()


def port():
    return int(os.getenv("PYCERNAN_AVRO_PORT", "2002"))
