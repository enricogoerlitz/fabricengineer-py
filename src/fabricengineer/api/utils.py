import json
import time
import base64
import requests

from fabricengineer.api.fabric.client.fabric import fabric_client
from fabricengineer.logging.logger import logger


def base64_encode(obj: dict | str | bytes | bytearray) -> str:
    """
    Encodiert dicts (als JSON), Strings (UTF-8) oder rohe Bytes/Bytearray
    zu Base64 (ASCII-String).
    """
    if isinstance(obj, dict):
        obj = json.dumps(obj, ensure_ascii=True).encode("utf-8")
    elif isinstance(obj, str):
        obj = obj.encode("utf-8")
    elif isinstance(obj, (bytes, bytearray)):
        obj = bytes(obj)
    else:
        raise TypeError(f"Unsupported type: {type(obj)!r}")

    return base64.b64encode(obj).decode("ascii")


def base64_encode_zip(filepath: str) -> str:
    with open(filepath, "rb") as zf:
        zip_bytes = zf.read()
    return base64_encode(zip_bytes)


# def base64_encode_zip(filepath: str) -> str:
#     with zipfile.ZipFile(filepath, "r") as zf:
#         zip_bytes = io.BytesIO()
#         for name in zf.namelist():
#             zip_bytes.write(zf.read(name))
#         return base64_encode(zip_bytes.getvalue())


def base64_decode(obj_str: str):
    decoded = base64.b64decode(obj_str).decode("utf-8")
    return decoded


def _retry_after(resp: requests.Response, retry_max_seconds: int = 5) -> int:
    return min(int(resp.headers.get("Retry-After", retry_max_seconds)), retry_max_seconds)


def http_wait_for_completion_after_202(
        resp: requests.Response,
        payload: dict = None,
        retry_max_seconds: int = 5,
        timeout: int = 90
) -> requests.Response:
    op_id = resp.headers["x-ms-operation-id"]
    op_location = resp.headers["Location"]
    retry = _retry_after(resp, retry_max_seconds)
    logger.info(f"Status=202, Operation ID: {op_id}, Location: {op_location}, Retry after: {retry}s")

    retry_sum = 0
    obj = None
    while True:
        time.sleep(retry)
        resp_retry = requests.get(op_location, headers=fabric_client.headers)

        if resp_retry.json()["status"] == "Succeeded":
            res = requests.get(resp_retry.headers["Location"], headers=fabric_client.headers)
            res.raise_for_status()
            obj = res.json()
            break

        retry = _retry_after(resp_retry)
        retry_sum += retry
        if retry_sum > timeout:
            logger.warn(f"Timeout after {timeout}s")
            raise TimeoutError(f"Timeout while waiting for item creation. Payload: {payload}")

        logger.info(f"Wait for more {retry}s")

    return obj
