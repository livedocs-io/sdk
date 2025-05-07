import base64
import decimal
import uuid
from datetime import date, datetime, time


def serializer(obj):
    """
    Serializes an object to a JSON-compatible format.
    """

    if obj is None:
        return None
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("utf-8")
    elif isinstance(obj, dict):
        return {k: serializer(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serializer(item) for item in obj]
    elif isinstance(obj, (set, tuple, frozenset)):
        return [serializer(item) for item in obj]
    elif isinstance(obj, complex):
        return {"real": obj.real, "imag": obj.imag}
    else:
        return str(obj)
