import requests


from typing import Literal


def import_module(
        name: Literal[
            "transform.mlv",
            "transform.silver.insertonly",
            "transform.silver.sdc2"
        ],
        version: str
) -> str:
    base_path = f"https://raw.githubusercontent.com/enricogoerlitz/fabricengineer-py/refs/tags/{version}/src/fabricengineer"

    module_map = {
        "transform.mlv": _import_module_mlv,
        "transform.silver.insertonly": _import_module_insertonly,
        "transform.silver.sdc2": _import_module_sdc2
    }

    if name not in module_map:
        raise ValueError(f"Unknown module: {name}")

    return module_map[name](base_path, version)


def _import_module_insertonly(base_path: str, version: str) -> str:
    lakehouse_module = _import_transform_lakehouse_module(base_path)
    base_module = _import_transform_silver_base_module(base_path)
    utils_module = _import_transform_silver_utils_module(base_path)
    insertonly_module = _import_transform_silver_insertonly_module(base_path)

    imports = """
import os

from datetime import datetime
from typing import Callable
from abc import ABC, abstractmethod
from dataclasses import dataclass
from uuid import uuid4

from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
    types as T,
    Window
)
"""
    code = "\n\n\n".join([
        imports,
        lakehouse_module,
        utils_module,
        base_module,
        insertonly_module
    ])

    return code


def _import_module_sdc2(version: str) -> str:
    pass


def _import_module_mlv(version: str) -> str:
    pass


def _import_transform_lakehouse_module(base_path: str) -> str:
    lakehouse_module = f"{base_path}/transform/lakehouse.py"
    return _fetch_module_content(lakehouse_module)


def _import_transform_silver_base_module(base_path: str) -> str:
    base_module = f"{base_path}/transform/silver/base.py"
    return _fetch_module_content(base_module)


def _import_transform_silver_utils_module(base_path: str) -> str:
    utils_module = f"{base_path}/transform/silver/utils.py"
    return _fetch_module_content(utils_module)


def _import_transform_silver_insertonly_module(base_path: str) -> str:
    insertonly_module = f"{base_path}/transform/silver/insertonly.py"
    return _fetch_module_content(insertonly_module)


def _fetch_module_content(module_path: str) -> str:
    resp = requests.get(module_path)
    assert resp.status_code == 200, f"Failed to fetch module: {module_path}"

    code = resp.text.split(_filename(module_path))
    if not code or len(code) < 2:
        raise ValueError(
            (f"Module content is malformed: {module_path}."),
            f"Content: {resp.text}"
        )

    code = code[1].strip()
    return code


def _filename(path: str) -> str:
    return path.split("/")[-1]
