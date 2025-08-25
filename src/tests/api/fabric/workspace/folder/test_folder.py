import pytest
import requests

from fabricengineer.api.fabric.client.fabric import set_global_fabric_client, get_env_svc
from fabricengineer.api.fabric.workspace.folder import (
    WorkspaceFolder,
    WorkspaceFolderAPIData
)
from tests.utils import rand_workspace_item_name


class TestWorkspaceFolderAPIData:
    """Test WorkspaceFolderAPIData dataclass."""

    def test_create_with_required_fields(self):
        """Test creating WorkspaceFolderAPIData with only required fields."""
        api_data = WorkspaceFolderAPIData(
            id="workspace_folder-123",
            workspaceId="workspace-123",
            displayName="Test WorkspaceFolder",
            parentFolderId="parent-folder-123"
        )

        assert api_data.id == "workspace_folder-123"
        assert api_data.displayName == "Test WorkspaceFolder"
        assert api_data.parentFolderId == "parent-folder-123"

    def test_create_with_all_fields(self):
        """Test creating WorkspaceFolderAPIData with all fields."""

        api_data = WorkspaceFolderAPIData(
            id="workspace_folder-456",
            displayName="Full Test WorkspaceFolder",
            workspaceId="workspace-123",
            parentFolderId="parent-folder-456"
        )

        assert api_data.id == "workspace_folder-456"
        assert api_data.displayName == "Full Test WorkspaceFolder"
        assert api_data.parentFolderId == "parent-folder-456"
        assert api_data.workspaceId == "workspace-123"

    def test_required_attributes_exist(self):
        """Test that all required attributes exist."""
        api_data = WorkspaceFolderAPIData(
            id="test-id",
            workspaceId="workspace-123",
            displayName="Test Name",
            parentFolderId=None
        )

        # Required attributes from BaseItemAPIData
        assert hasattr(api_data, 'id')
        assert hasattr(api_data, 'displayName')


class TestWorkspaceFolder:
    test_f: WorkspaceFolder = None

    def authenticate(self) -> None:
        set_global_fabric_client(get_env_svc())

    def rand_workspace_folder(self, workspace_id: str, folder: WorkspaceFolder = None) -> WorkspaceFolder:
        name = rand_workspace_item_name("F")
        return WorkspaceFolder(
            workspace_id=workspace_id,
            name=name,
            parent_folder=folder
        )

    def test_init_workspace_folder(self, workspace_id: str):
        workspace_folder: WorkspaceFolder = self.rand_workspace_folder(workspace_id)
        assert workspace_folder.item.fields.get("displayName", "").startswith("F_")
        assert workspace_folder.item.fields.get("parentFolderId") is None

    def test_from_json(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        json_data = {
            "workspaceId": workspace_id,
            "displayName": "WP_Test",
            "id": "12345",
            "parentFolderId": folder_singleton
        }
        obj = WorkspaceFolder.from_json(json_data)
        assert obj.item.fields.get("displayName") == json_data["displayName"]
        assert obj.item.fields.get("parentFolderId") == json_data["parentFolderId"]
        assert obj.item.api.displayName == json_data["displayName"]
        assert obj.item.api.id == json_data["id"]

    def test_create(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_workspace_folder(workspace_id)
        obj.create()
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.workspaceId == workspace_id
        assert obj.item.api.parentFolderId is None

    def test_create_with_parent(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        obj = self.rand_workspace_folder(workspace_id, folder_singleton)
        obj.create()
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.workspaceId == workspace_id
        assert obj.item.api.parentFolderId == folder_singleton.item.api.id

    def test_update(self, folder_singleton: WorkspaceFolder):
        self.authenticate()
        new_name = rand_workspace_item_name("F")
        assert folder_singleton.item.api.parentFolderId is None
        folder_singleton.update(displayName=new_name)
        assert folder_singleton.item.api.displayName == new_name

    def test_fetch_and_delete(self, folder_singleton: WorkspaceFolder):
        self.authenticate()
        folder_singleton.fetch()
        folder_singleton.delete()
        with pytest.raises(requests.HTTPError):
            folder_singleton.fetch()

    def test_get_by_name(self, folder_singleton: WorkspaceFolder):
        self.authenticate()
        fetched_obj = WorkspaceFolder.get_by_name(
            folder_singleton.item.api.workspaceId,
            folder_singleton.item.api.displayName
        )
        assert fetched_obj.item.api.id == folder_singleton.item.api.id

    def test_get_by_id(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        fetched_obj = WorkspaceFolder.get_by_id(workspace_id, folder_singleton.item.api.id)
        assert fetched_obj.item.api.id == folder_singleton.item.api.id

    def test_list(self, workspace_id: str):
        self.authenticate()
        workspace_folders = WorkspaceFolder.list(workspace_id)
        assert isinstance(workspace_folders, list)
        assert len(workspace_folders) > 0
        for obj in workspace_folders:
            assert isinstance(obj, WorkspaceFolder)
            assert obj.item.api.id is not None
            assert obj.item.api.displayName is not None

    def test_exists(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        obj = self.rand_workspace_folder(workspace_id)
        assert not obj.exists()
        assert folder_singleton.exists()

    def test_create_if_not_exists(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_workspace_folder(workspace_id)
        assert not obj.exists()
        obj.create_if_not_exists()
        assert obj.exists()
        obj.create_if_not_exists()

    def test_fetch_definition(self, folder_singleton: WorkspaceFolder):
        self.authenticate()
        with pytest.raises(requests.HTTPError):
            folder_singleton.fetch_definition()
