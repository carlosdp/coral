import base64
import cloudpickle

SERIALIZATION_VERSION = "cloudpickle-v1"


def dumps(obj: object) -> str:
    payload = cloudpickle.dumps(obj)
    return base64.b64encode(payload).decode("utf-8")


def loads(payload_b64: str) -> object:
    raw = base64.b64decode(payload_b64.encode("utf-8"))
    return cloudpickle.loads(raw)
