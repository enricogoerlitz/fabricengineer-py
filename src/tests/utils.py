import io
import os
import logging
import uuid

from typing import Any, Callable
from contextlib import redirect_stdout, contextmanager
from fabricengineer.api.fabric.client.fabric import set_global_fabric_client, get_env_svc
from fabricengineer.logging.logger import logger


def authenticate() -> None:
    set_global_fabric_client(get_env_svc())


def rand_workspace_item_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex.replace('-', '')}"


class NotebookUtilsFSMock:
    def _get_path(self, file: str) -> str:
        return os.path.join(os.getcwd(), file)

    def exists(self, path: str) -> bool:
        return os.path.exists(self._get_path(path))

    def put(
        self,
        file: str,
        content: str,
        overwrite: bool = False
    ) -> None:
        path = self._get_path(file)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if os.path.exists(path) and not overwrite:
            raise FileExistsError(f"File {path} already exists and overwrite is set to False.")
        with open(path, 'w') as f:
            f.write(content)


class NotebookUtilsMock:
    def __init__(self):
        self.fs = NotebookUtilsFSMock()


def mount_py_file(file_path: str) -> None:
    """Mount a Python file to the current namespace."""
    with open(file_path) as f:
        code = f.read()
    exec(code, globals(), locals())
    return locals().get('mlv', None)


@contextmanager
def capture_logs(logger: logging.Logger):
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(filename)s %(message)s", "%d.%m.%Y %H:%M:%S,%f")
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    try:
        yield log_stream
    finally:
        handler.close()
        logger.removeHandler(handler)


def sniff_logs(fn: Callable[[], Any]) -> tuple[Any, list[str]]:
    with capture_logs(logger) as log_stream:
        result = fn()
    logs = log_stream.getvalue().splitlines()
    return result, logs


def sniff_print_logs(fn: callable) -> tuple[Any, list[str]]:
    log_stream = io.StringIO()
    with redirect_stdout(log_stream):
        result = fn()
    logs = log_stream.getvalue().splitlines()
    return result, logs
