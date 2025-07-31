import io
import os

from typing import Any
from contextlib import redirect_stdout


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


def sniff_logs(fn: callable) -> tuple[Any, list[str]]:
    log_stream = io.StringIO()
    with redirect_stdout(log_stream):
        result = fn()
    logs = log_stream.getvalue().splitlines()
    return result, logs
