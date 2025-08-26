import pytest
import requests

from fabricengineer.api.fabric.client.fabric import set_global_fabric_client, get_env_svc
from fabricengineer.api.fabric.workspace.items.lakehouse import Lakehouse
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from tests.utils import rand_workspace_item_name


@pytest.fixture
def lakehouse_singleton(workspace_id: str):
    """Fixture to create a Lakehouse instance."""
    set_global_fabric_client(get_env_svc())
    name = rand_workspace_item_name("LH")
    lakehouse = Lakehouse(
        workspace_id=workspace_id,
        name=name,
        description="Test Lakehouse"
    )
    lakehouse.create(wait_for_completion=False)
    return lakehouse


class TestLakehouse:

    def authenticate(self) -> None:
        set_global_fabric_client(get_env_svc())

    def rand_lakehouse(self, workspace_id: str, folder: WorkspaceFolder = None) -> Lakehouse:
        name = rand_workspace_item_name("LH")
        return Lakehouse(
            workspace_id=workspace_id,
            name=name,
            description="Test Lakehouse",
            folder=folder
        )

    def test_init_lakehouse(self, workspace_id: str):
        lakehouse: Lakehouse = self.rand_lakehouse(workspace_id)
        assert lakehouse.item.fields.get("displayName", "").startswith("LH_")
        assert lakehouse.item.fields.get("description") == "Test Lakehouse"

    def test_from_json(self):
        json_data = {
            "workspaceId": "<WorkspaceId>",
            "displayName": "WP_Test",
            "description": "Test Lakehouse from JSON",
            "id": "12345",
            "type": "Lakehouse",
            "properties": {
                "defaultSchema": "dbo",
                "oneLakeTablesPath": "<TablePath>",
                "oneLakeFilesPath": "<FilePath>",
                "sqlEndpointProperties": {
                    "id": "<SqlEndpointId>",
                    "connectionString": "<SqlConnectionString>",
                    "provisioningStatus": "<SqlProvisioningStatus>",
                }

            }
        }
        obj = Lakehouse.from_json(json_data)
        assert obj.item.fields.get("displayName") == json_data["displayName"]
        assert obj.item.fields.get("description") == json_data["description"]
        assert obj.item.api.displayName == json_data["displayName"]
        assert obj.item.api.description == json_data["description"]
        assert obj.item.api.id == json_data["id"]
        assert obj.item.api.type == json_data["type"]
        assert obj.item.api.properties.defaultSchema == json_data["properties"]["defaultSchema"]
        assert obj.item.api.properties.oneLakeTablesPath == json_data["properties"]["oneLakeTablesPath"]
        assert obj.item.api.properties.oneLakeFilesPath == json_data["properties"]["oneLakeFilesPath"]
        assert obj.item.api.properties.sqlEndpointProperties.id == json_data["properties"]["sqlEndpointProperties"]["id"]
        assert obj.item.api.properties.sqlEndpointProperties.connectionString == json_data["properties"]["sqlEndpointProperties"]["connectionString"]
        assert obj.item.api.properties.sqlEndpointProperties.provisioningStatus == json_data["properties"]["sqlEndpointProperties"]["provisioningStatus"]

    def test_create(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_lakehouse(workspace_id)
        obj.create()
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "Lakehouse"
        assert obj.item.api.properties.defaultSchema is not None
        assert obj.item.api.properties.oneLakeTablesPath is not None
        assert obj.item.api.properties.oneLakeFilesPath is not None
        assert obj.item.api.properties.sqlEndpointProperties.id is not None
        assert obj.item.api.properties.sqlEndpointProperties.connectionString is not None
        assert obj.item.api.properties.sqlEndpointProperties.provisioningStatus is not None

    def test_create_in_folder(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        obj = self.rand_lakehouse(workspace_id, folder_singleton)
        obj.create(wait_for_completion=False)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "Lakehouse"

    def test_update(self, lakehouse_singleton: Lakehouse):
        self.authenticate()
        assert lakehouse_singleton.item.api.description == "Test Lakehouse"
        lakehouse_singleton.update(description="Updated Description")
        assert lakehouse_singleton.item.api.description == "Updated Description"

    def test_fetch_and_delete(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_lakehouse(workspace_id)
        obj.create(wait_for_completion=False)
        obj.fetch()
        obj.delete()
        with pytest.raises(requests.HTTPError):
            obj.fetch()

    def test_get_by_name(self, lakehouse_singleton: Lakehouse):
        self.authenticate()
        fetched_obj = Lakehouse.get_by_name(
            lakehouse_singleton.item.api.workspaceId,
            lakehouse_singleton.item.api.displayName
        )
        assert fetched_obj.item.api.id == lakehouse_singleton.item.api.id

    def test_get_by_id(self, workspace_id: str, lakehouse_singleton: Lakehouse):
        self.authenticate()
        fetched_obj = Lakehouse.get_by_id(workspace_id, lakehouse_singleton.item.api.id)
        assert fetched_obj.item.api.id == lakehouse_singleton.item.api.id

    def test_list(self, workspace_id: str):
        self.authenticate()
        lakehouses = Lakehouse.list(workspace_id)
        assert isinstance(lakehouses, list)
        assert len(lakehouses) > 0
        for obj in lakehouses:
            assert isinstance(obj, Lakehouse)
            assert obj.item.api.id is not None
            assert obj.item.api.displayName is not None
            assert obj.item.api.description is not None

    def test_exists(self, workspace_id: str, lakehouse_singleton: Lakehouse):
        self.authenticate()
        obj = self.rand_lakehouse(workspace_id)
        assert not obj.exists()
        assert lakehouse_singleton.exists()

    def test_create_if_not_exists(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_lakehouse(workspace_id)
        assert not obj.exists()
        obj.create_if_not_exists(wait_for_completion=False)
        assert obj.exists()
        obj.create_if_not_exists()

    def test_fetch_definition(self, lakehouse_singleton: Lakehouse):
        self.authenticate()
        with pytest.raises(requests.HTTPError):
            lakehouse_singleton.fetch_definition()
