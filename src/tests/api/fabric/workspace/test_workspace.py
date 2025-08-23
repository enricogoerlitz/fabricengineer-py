import pytest
import uuid
import requests

from fabricengineer.api.fabric.workspace.workspace import (
    Workspace,
    WorkspaceAPIData,
    WorkspaceOneLakeEndpoints
)
from fabricengineer.api.fabric.client.fabric import get_env_svc, set_global_fabric_client


class TestWorkspaceOneLakeEndpoints:
    """Test WorkspaceOneLakeEndpoints dataclass."""

    def test_create_with_default_values(self):
        """Test creating WorkspaceOneLakeEndpoints with default values."""
        endpoints = WorkspaceOneLakeEndpoints()

        assert endpoints.blobEndpoint is None
        assert endpoints.dfsEndpoint is None

    def test_create_with_values(self):
        """Test creating WorkspaceOneLakeEndpoints with specific values."""
        blob_endpoint = "https://example.blob.core.windows.net"
        dfs_endpoint = "https://example.dfs.core.windows.net"

        endpoints = WorkspaceOneLakeEndpoints(
            blobEndpoint=blob_endpoint,
            dfsEndpoint=dfs_endpoint
        )

        assert endpoints.blobEndpoint == blob_endpoint
        assert endpoints.dfsEndpoint == dfs_endpoint

    def test_attributes_exist(self):
        """Test that all required attributes exist."""
        endpoints = WorkspaceOneLakeEndpoints()

        assert hasattr(endpoints, 'blobEndpoint')
        assert hasattr(endpoints, 'dfsEndpoint')


class TestWorkspaceAPIData:
    """Test WorkspaceAPIData dataclass."""

    def test_create_with_required_fields(self):
        """Test creating WorkspaceAPIData with only required fields."""
        api_data = WorkspaceAPIData(
            id="workspace-123",
            displayName="Test Workspace",
            description="Test Description",
            type="Workspace"
        )

        assert api_data.id == "workspace-123"
        assert api_data.displayName == "Test Workspace"
        assert api_data.description == "Test Description"
        assert api_data.type == "Workspace"
        assert api_data.capacityId is None
        assert api_data.capacityRegion is None
        assert api_data.capacityAssignmentProgress is None
        assert isinstance(api_data.oneLakeEndpoints, WorkspaceOneLakeEndpoints)

    def test_create_with_all_fields(self):
        """Test creating WorkspaceAPIData with all fields."""
        endpoints = WorkspaceOneLakeEndpoints(
            blobEndpoint="https://example.blob.core.windows.net",
            dfsEndpoint="https://example.dfs.core.windows.net"
        )

        api_data = WorkspaceAPIData(
            id="workspace-456",
            displayName="Full Test Workspace",
            description="Full test description",
            type="Workspace",
            capacityId="capacity-789",
            capacityRegion="West Europe",
            capacityAssignmentProgress="Completed",
            oneLakeEndpoints=endpoints
        )

        assert api_data.id == "workspace-456"
        assert api_data.displayName == "Full Test Workspace"
        assert api_data.description == "Full test description"
        assert api_data.type == "Workspace"
        assert api_data.capacityId == "capacity-789"
        assert api_data.capacityRegion == "West Europe"
        assert api_data.capacityAssignmentProgress == "Completed"
        assert api_data.oneLakeEndpoints == endpoints
        assert api_data.oneLakeEndpoints.blobEndpoint == "https://example.blob.core.windows.net"
        assert api_data.oneLakeEndpoints.dfsEndpoint == "https://example.dfs.core.windows.net"

    def test_default_one_lake_endpoints_factory(self):
        """Test that oneLakeEndpoints has default factory."""
        api_data1 = WorkspaceAPIData(
            id="workspace-1",
            displayName="Workspace 1",
            description="Description 1",
            type="Workspace"
        )

        api_data2 = WorkspaceAPIData(
            id="workspace-2",
            displayName="Workspace 2",
            description="Description 2",
            type="Workspace"
        )

        # Each instance should have its own oneLakeEndpoints object
        assert api_data1.oneLakeEndpoints is not api_data2.oneLakeEndpoints
        assert isinstance(api_data1.oneLakeEndpoints, WorkspaceOneLakeEndpoints)
        assert isinstance(api_data2.oneLakeEndpoints, WorkspaceOneLakeEndpoints)

    def test_required_attributes_exist(self):
        """Test that all required attributes exist."""
        api_data = WorkspaceAPIData(
            id="test-id",
            displayName="Test Name",
            description="Test Description",
            type="Test Type"
        )

        # Required attributes
        assert hasattr(api_data, 'id')
        assert hasattr(api_data, 'displayName')
        assert hasattr(api_data, 'description')
        assert hasattr(api_data, 'type')

        # Optional attributes
        assert hasattr(api_data, 'capacityId')
        assert hasattr(api_data, 'capacityRegion')
        assert hasattr(api_data, 'capacityAssignmentProgress')
        assert hasattr(api_data, 'oneLakeEndpoints')

    def test_workspace_api_data_none_values(self):
        """Test WorkspaceAPIData with None values for optional fields."""
        api_data = WorkspaceAPIData(
            id="workspace-123",
            displayName="Test Workspace",
            description="Test Description",
            type="Workspace",
            capacityId=None,
            capacityRegion=None,
            capacityAssignmentProgress=None,
            oneLakeEndpoints=None
        )

        assert api_data.capacityId is None
        assert api_data.capacityRegion is None
        assert api_data.capacityAssignmentProgress is None
        assert api_data.oneLakeEndpoints is None


class TestWorkspace:
    def authenticate(self) -> None:
        set_global_fabric_client(get_env_svc())

    def rand_workspace(self) -> Workspace:
        name = f"WP_{uuid.uuid4().hex[:8].replace('-', '')}"
        return Workspace(
            name=name,
            description="Test Workspace",
        )

    def test_init_workspace(self):
        workspace: Workspace = self.rand_workspace()
        assert workspace.item.fields.get("displayName", "").startswith("WP_")
        assert workspace.item.fields.get("description") == "Test Workspace"

    def test_from_json(self):
        json_data = {
            "displayName": "WP_Test",
            "description": "Test Workspace from JSON",
            "id": "12345",
            "type": "workspace",
            "capacityAssignmentProgress": "Success",
            "oneLakeEndpoints": {
                "blobEndpoint": "https://example.blob.core.windows.net",
                "dfsEndpoint": "https://example.dfs.core.windows.net"
            }
        }
        ws = Workspace.from_json(json_data)
        assert ws.item.fields.get("displayName") == json_data["displayName"]
        assert ws.item.fields.get("description") == json_data["description"]
        assert ws.item.api.displayName == json_data["displayName"]
        assert ws.item.api.description == json_data["description"]
        assert ws.item.api.id == json_data["id"]
        assert ws.item.api.type == json_data["type"]
        assert ws.item.api.capacityAssignmentProgress == json_data["capacityAssignmentProgress"]
        assert ws.item.api.oneLakeEndpoints.blobEndpoint == json_data["oneLakeEndpoints"]["blobEndpoint"]
        assert ws.item.api.oneLakeEndpoints.dfsEndpoint == json_data["oneLakeEndpoints"]["dfsEndpoint"]

    def test_create(self):
        self.authenticate()
        ws = self.rand_workspace()
        ws.create()
        assert ws.item.api.id is not None
        assert ws.item.api.displayName == ws.item.fields.get("displayName")
        assert ws.item.api.description == ws.item.fields.get("description")
        assert ws.item.api.type == "Workspace"
        assert ws.item.api.capacityId is None
        assert ws.item.api.capacityRegion is None
        assert ws.item.api.capacityAssignmentProgress is not None
        assert ws.item.api.oneLakeEndpoints is not None

    def test_update(self, workspace: Workspace):
        self.authenticate()
        old_description = workspace.item.api.description
        new_description = f"Updated Description {uuid.uuid4().hex[:8].replace('-', '')}"
        workspace.update(description=new_description)
        assert workspace.item.api.description == new_description
        assert workspace.item.api.description != old_description

    def test_fetch_and_delete(self):
        self.authenticate()
        ws = self.rand_workspace()
        ws.create()
        ws.fetch()
        ws.delete()
        with pytest.raises(requests.HTTPError):
            ws.fetch()

    def test_get_by_name(self, workspace: Workspace):
        self.authenticate()
        fetched_ws = Workspace.get_by_name(workspace.item.api.displayName)
        assert fetched_ws.item.api.id == workspace.item.api.id

    def test_get_by_id(self, workspace: Workspace):
        self.authenticate()
        fetched_ws = Workspace.get_by_id(workspace.item.api.id)
        assert fetched_ws.item.api.id == workspace.item.api.id

    def test_list(self):
        self.authenticate()
        workspaces = Workspace.list()
        assert isinstance(workspaces, list)
        assert len(workspaces) > 0
        for ws in workspaces:
            assert isinstance(ws, Workspace)
            assert ws.item.api.id is not None
            assert ws.item.api.displayName is not None
            assert ws.item.api.description is not None

    def test_exists(self):
        self.authenticate()
        ws = self.rand_workspace()
        assert not ws.exists()
        ws.create()
        assert ws.exists()

    def test_create_if_not_exists(self):
        self.authenticate()
        ws = self.rand_workspace()
        assert not ws.exists()
        ws.create_if_not_exists()
        assert ws.exists()
        ws.create_if_not_exists()

    def test_fetch_definition(self, workspace: Workspace):
        self.authenticate()
        # workspace.create()
        with pytest.raises(requests.HTTPError):
            workspace.fetch_definition()
