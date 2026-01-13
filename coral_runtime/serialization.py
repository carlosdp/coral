import base64
import cloudpickle


def dumps(obj: object) -> bytes:
    payload = cloudpickle.dumps(obj)
    return base64.b64encode(payload)


def loads(payload_b64: str) -> object:
    raw = base64.b64decode(payload_b64.encode("utf-8"))
    return cloudpickle.loads(raw)
