import sys


def write(message: str) -> None:
    sys.stdout.write(message + "\n")
    sys.stdout.flush()
