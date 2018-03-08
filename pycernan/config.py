import os


def host():
    return os.getenv("PYCERNAN_HOST", "localhost")
