import base64
import decimal
import uuid
from datetime import date, datetime, time


def _json_serializer(obj):
    """
    Serialize an object to JSON.
    """
    if obj is None:
        return None
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
    elif isinstance(obj, float):
        return str(obj)
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("utf-8")
    elif isinstance(obj, dict):
        return {k: _json_serializer(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_json_serializer(item) for item in obj]
    elif isinstance(obj, (set, tuple, frozenset)):
        return [_json_serializer(item) for item in obj]
    elif isinstance(obj, complex):
        return {"real": obj.real, "imag": obj.imag}
    else:
        return str(obj)
