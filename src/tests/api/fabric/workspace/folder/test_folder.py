import pytest
import uuid
import requests

from fabricengineer.api.fabric.client.fabric import set_global_fabric_client, get_env_svc
from fabricengineer.api.fabric.workspace.folder import (
    WorkspaceFolder,
    WorkspaceFolderAPIData
)


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

    def rand_workspace_folder(self, workspace_id: str) -> WorkspaceFolder:
        name = f"F_{uuid.uuid4().hex[:8].replace('-', '')}"
        return WorkspaceFolder(
            workspace_id=workspace_id,
            name=name
        )

    def workspace_folder_singleton(self, workspace_id: str) -> WorkspaceFolder:
        if self.test_f is None or not self.test_f.exists():
            self.test_f = self.rand_workspace_folder(workspace_id)
            self.test_f.create()
        return self.test_f

    def test_init_workspace_folder(self, workspace_id: str):
        workspace_folder: WorkspaceFolder = self.rand_workspace_folder(workspace_id)
        assert workspace_folder.item.fields.get("displayName", "").startswith("F_")
        assert workspace_folder.item.fields.get("parentFolderId") is None

    def test_from_json(self, workspace_id: str):
        json_data = {
            "workspaceId": workspace_id,
            "displayName": "WP_Test",
            "id": "12345",
            "parentFolderId": "<ParentFolderId>"
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

    def test_create_with_parent(self, workspace_id: str):
        self.authenticate()
        parent_folder = self.workspace_folder_singleton(workspace_id)
        obj = self.rand_workspace_folder(workspace_id)
        obj.item.fields["parentFolderId"] = parent_folder.item.api.id
        obj.create()
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.workspaceId == workspace_id
        assert obj.item.api.parentFolderId == parent_folder.item.api.id

    def test_update(self, workspace_id: str):
        self.authenticate()
        obj = self.workspace_folder_singleton(workspace_id)
        new_name = f"F_{uuid.uuid4().hex[:8].replace('-', '')}"
        assert obj.item.api.parentFolderId is None
        obj.update(displayName=new_name)
        assert obj.item.api.displayName == new_name

    def test_fetch_and_delete(self, workspace_id: str):
        self.authenticate()
        obj = self.workspace_folder_singleton(workspace_id)
        obj.fetch()
        obj.delete()
        with pytest.raises(requests.HTTPError):
            obj.fetch()

    def test_get_by_name(self, workspace_id: str):
        self.authenticate()
        obj = self.workspace_folder_singleton(workspace_id)
        fetched_obj = WorkspaceFolder.get_by_name(workspace_id, obj.item.api.displayName)
        assert fetched_obj.item.api.id == obj.item.api.id

    def test_get_by_id(self, workspace_id: str):
        self.authenticate()
        obj = self.workspace_folder_singleton(workspace_id)
        fetched_obj = WorkspaceFolder.get_by_id(workspace_id, obj.item.api.id)
        assert fetched_obj.item.api.id == obj.item.api.id

    def test_list(self, workspace_id: str):
        self.authenticate()
        workspace_folders = WorkspaceFolder.list(workspace_id)
        assert isinstance(workspace_folders, list)
        assert len(workspace_folders) > 0
        for obj in workspace_folders:
            assert isinstance(obj, WorkspaceFolder)
            assert obj.item.api.id is not None
            assert obj.item.api.displayName is not None

    def test_exists(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_workspace_folder(workspace_id)
        assert not obj.exists()
        obj.create()
        assert obj.exists()

    def test_create_if_not_exists(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_workspace_folder(workspace_id)
        assert not obj.exists()
        obj.create_if_not_exists()
        assert obj.exists()
        obj.create_if_not_exists()

    def test_fetch_definition(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_workspace_folder(workspace_id)
        obj.create()
        with pytest.raises(requests.HTTPError):
            obj.fetch_definition()
