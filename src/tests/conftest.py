import os
import shutil
import pytest
import time
import requests

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

from fabricengineer.api.auth import MicrosoftExtraSVC
from fabricengineer.api.fabric.client.fabric import FabricAPIClient, set_global_fabric_client
from fabricengineer.api.fabric.workspace.workspace import Workspace
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from fabricengineer.api.fabric.workspace.items import (
    Lakehouse,
    Warehouse,
    DataPipeline,
    VariableLibrary,
    Notebook
)
from tests.utils import NotebookUtilsMock


load_dotenv(".env")


svc = MicrosoftExtraSVC(
    tenant_id=os.getenv("MICROSOFT_TENANT_ID"),
    client_id=os.getenv("SVC_MICROSOFT_FABRIC_CLIENT_ID"),
    client_secret=os.getenv("SVC_MICROSOFT_FABRIC_SECRET_VALUE")
)

assert len(svc.tenant_id) == 36
assert len(svc.client_id) == 36
assert len(svc.client_secret) == 40


@pytest.fixture(scope="function")
def spark_():
    builder = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def notebookutils_():
    """Create a mock for NotebookUtils."""
    return NotebookUtilsMock()


@pytest.fixture(scope="session")
def fabric_client():
    """Create a mock for Fabric API Client."""
    return FabricAPIClient(api_version="v1")


@pytest.fixture(scope="session")
def workspace_id():
    workspace_id = os.getenv("WORKSPACE_ID")
    assert isinstance(workspace_id, str) and len(workspace_id) > 0, "WORKSPACE_ID must be set in the environment variables."
    return workspace_id


@pytest.fixture(scope="session")
def workspace():
    set_global_fabric_client(svc)
    workspace_id = os.getenv("WORKSPACE_ID")
    workspace = Workspace.get_by_id(workspace_id)
    assert isinstance(workspace_id, str) and len(workspace_id) > 0, "WORKSPACE_ID must be set in the environment variables."
    return workspace


@pytest.fixture
def folder_singleton(workspace_id: str):
    """Fixture to create a Folder instance."""
    set_global_fabric_client(svc)
    name = "F_TESTING_FOLDER"
    folder = WorkspaceFolder(
        workspace_id=workspace_id,
        name=name
    )
    folder.create_if_not_exists()
    return folder


@pytest.fixture
def msf_svc():
    """Create a mock for Microsoft Fabric Service."""
    set_global_fabric_client(svc)
    return svc


@pytest.fixture(scope="session", autouse=True)
def global_cleanup_fs():
    yield  # alle Tests laufen zuerst

    print("CLEANUP: Removing temporary directories and files.")

    def cleanup_fs():
        path_tmp = "tmp"
        path_Files = "Files"

        rm_paths = [path_Files, path_tmp]
        for path in rm_paths:
            if os.path.exists(path):
                shutil.rmtree(path)

    cleanup_fs()


@pytest.fixture(scope="session", autouse=True)
def global_cleanup_fabric_workspaces():
    yield  # alle Tests laufen zuerst

    print("CLEANUP: Removing all temporary fabric workspaces.")

    set_global_fabric_client(svc)

    def cleanup_workspaces():
        workspaces = [ws for ws in Workspace.list() if ws.item.api.displayName.startswith("WP_")]
        for ws in workspaces:
            ws.delete()

    cleanup_workspaces()


@pytest.fixture(scope="session", autouse=True)
def global_cleanup_fabric_items():
    yield  # alle Tests laufen zuerst

    print("CLEANUP: Removing all temporary fabric items.")

    set_global_fabric_client(svc)
    workspace_id = os.getenv("WORKSPACE_ID")

    def cleanup_folders():
        rate_limit_wait_seconds = 60
        folders = WorkspaceFolder.list(workspace_id)
        deleted_folders = set()
        while len(deleted_folders) != len(folders):
            try:
                has_child_folder_ids = {
                    folder.item.api.parentFolderId
                    for folder in folders
                    if folder.item.api.parentFolderId and folder not in deleted_folders
                }
                for folder in folders:
                    if folder in deleted_folders:
                        continue
                    if folder.item.api.id in has_child_folder_ids:
                        continue
                    print("Delete folder:", folder.item.api.displayName)
                    folder.delete()
                    deleted_folders.add(folder)
            except requests.HTTPError as e:
                if e.response.status_code == 429:
                    print(f"429 Too Many Requests: Wait for {rate_limit_wait_seconds} seconds")
                    time.sleep(rate_limit_wait_seconds)
                elif e.response.status_code == 404:
                    deleted_folders.add(folder)
                elif e.response.status_code == 400 and "FolderNotEmpty" in e.response.text:
                    deleted_folders.add(folder)
                else:
                    raise e
            except Exception as e:
                raise e

    def cleanup_items():
        items = (
            Lakehouse.list(workspace_id) +
            Warehouse.list(workspace_id) +
            DataPipeline.list(workspace_id) +
            Notebook.list(workspace_id) +
            VariableLibrary.list(workspace_id)
        )
        for item in items:
            item.delete()

    cleanup_items()
    cleanup_folders()
