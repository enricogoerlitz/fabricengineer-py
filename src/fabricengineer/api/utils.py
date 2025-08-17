import json
import base64


def base64_encode(obj: dict) -> str:
    json_bytes = json.dumps(obj, ensure_ascii=True).encode("utf-8")
    obj = base64.b64encode(json_bytes).decode("ascii")
    return obj
