import time
import functools
import requests

from typing import Iterable, Callable, Any, Optional

from fabricengineer.logging import logger


def retry(count: int = 3, on: Optional[Iterable[int]] = None, delay: int = 15) -> Callable:
    """
    Retry decorator for functions that return a requests.Response.
    Retries when response.status_code is in `on`.

    Args:
        count: Max. Anzahl der Versuche (inkl. des ersten Aufrufs).
        on: Iterable von Statuscodes, die einen Retry auslösen (Default: [429]).
        delay: Basiswartezeit in Sekunden zwischen den Versuchen, wenn kein Retry-After vorhanden ist.
    """
    on_set = set(on or [429])

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_resp: Optional[requests.Response] = None
            for attempt in range(1, count + 1):
                resp = func(*args, **kwargs)
                last_resp = resp

                if not isinstance(resp, requests.Response):
                    return resp

                if resp.status_code not in on_set:
                    return resp

                if attempt < count:
                    wait_seconds = delay
                    ra = resp.headers.get("Retry-After")
                    if ra is not None:
                        try:
                            wait_seconds = min(int(ra), 60)
                        except ValueError:
                            pass
                    logger.info(f"Request failed with {resp.status_code}. Retrying in {wait_seconds} seconds... (Attempt {attempt}/{count})")
                    time.sleep(wait_seconds)
                else:
                    return last_resp
            return last_resp
        return wrapper
    return decorator
