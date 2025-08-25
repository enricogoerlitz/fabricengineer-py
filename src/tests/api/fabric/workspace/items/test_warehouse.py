import pytest
import requests

from fabricengineer.api.fabric.client.fabric import set_global_fabric_client, get_env_svc
from fabricengineer.api.fabric.workspace.items.warehouse import (
    Warehouse,
    WarehouseAPIData,
    WarehouseProperties
)
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from tests.utils import rand_workspace_item_name


@pytest.fixture
def warehouse_singleton(workspace_id: str):
    """Fixture to create a Warehouse instance."""
    set_global_fabric_client(get_env_svc())
    name = rand_workspace_item_name("WH")
    warehouse = Warehouse(
        workspace_id=workspace_id,
        name=name,
        description="Test Warehouse"
    )
    warehouse.create()
    return warehouse


class TestWarehouseProperties:
    """Test WarehouseProperties dataclass."""

    def test_create_with_default_values(self):
        """Test creating WarehouseProperties with default values."""
        properties = WarehouseProperties()

        assert properties.connectionInfo is None
        assert properties.connectionString is None
        assert properties.createdDate is None
        assert properties.creationMode is None
        assert properties.sourceRestorePoint is None
        assert properties.collationType is None
        assert properties.lastUpdatedTime is None

    def test_create_with_values(self):
        """Test creating WarehouseProperties with specific values."""
        connection_info = "test-connection-info"
        connection_string = "Server=test;Database=test"
        created_date = "2023-01-01T10:00:00Z"
        creation_mode = "Manual"
        source_restore_point = "test-restore-point"
        collation_type = "Latin1_General_100_BIN2_UTF8"
        last_updated_time = "2023-01-02T10:00:00Z"

        properties = WarehouseProperties(
            connectionInfo=connection_info,
            connectionString=connection_string,
            createdDate=created_date,
            creationMode=creation_mode,
            sourceRestorePoint=source_restore_point,
            collationType=collation_type,
            lastUpdatedTime=last_updated_time
        )

        assert properties.connectionInfo == connection_info
        assert properties.connectionString == connection_string
        assert properties.createdDate == created_date
        assert properties.creationMode == creation_mode
        assert properties.sourceRestorePoint == source_restore_point
        assert properties.collationType == collation_type
        assert properties.lastUpdatedTime == last_updated_time

    def test_attributes_exist(self):
        """Test that all required attributes exist."""
        properties = WarehouseProperties()

        assert hasattr(properties, 'connectionInfo')
        assert hasattr(properties, 'connectionString')
        assert hasattr(properties, 'createdDate')
        assert hasattr(properties, 'creationMode')
        assert hasattr(properties, 'sourceRestorePoint')
        assert hasattr(properties, 'collationType')
        assert hasattr(properties, 'lastUpdatedTime')


class TestWarehouseAPIData:
    """Test WarehouseAPIData dataclass."""

    def test_create_with_required_fields(self):
        """Test creating WarehouseAPIData with only required fields."""
        api_data = WarehouseAPIData(
            id="warehouse-123",
            workspaceId="workspace-123",
            displayName="Test Warehouse",
            description="Test Description",
            type="Warehouse"
        )

        assert api_data.id == "warehouse-123"
        assert api_data.displayName == "Test Warehouse"
        assert api_data.description == "Test Description"
        assert api_data.type == "Warehouse"
        assert isinstance(api_data.properties, WarehouseProperties)

    def test_create_with_all_fields(self):
        """Test creating WarehouseAPIData with all fields."""
        properties = WarehouseProperties(
            connectionInfo="test-connection-info",
            connectionString="Server=test;Database=test",
            createdDate="2023-01-01T10:00:00Z",
            creationMode="Manual",
            sourceRestorePoint="test-restore-point",
            collationType="Latin1_General_100_BIN2_UTF8",
            lastUpdatedTime="2023-01-02T10:00:00Z"
        )

        api_data = WarehouseAPIData(
            id="warehouse-456",
            displayName="Full Test Warehouse",
            description="Full test description",
            type="Warehouse",
            workspaceId="workspace-123",
            properties=properties
        )

        assert api_data.id == "warehouse-456"
        assert api_data.displayName == "Full Test Warehouse"
        assert api_data.description == "Full test description"
        assert api_data.type == "Warehouse"
        assert api_data.workspaceId == "workspace-123"
        assert api_data.properties == properties
        assert api_data.properties.connectionInfo == "test-connection-info"
        assert api_data.properties.connectionString == "Server=test;Database=test"
        assert api_data.properties.createdDate == "2023-01-01T10:00:00Z"
        assert api_data.properties.creationMode == "Manual"
        assert api_data.properties.sourceRestorePoint == "test-restore-point"
        assert api_data.properties.collationType == "Latin1_General_100_BIN2_UTF8"
        assert api_data.properties.lastUpdatedTime == "2023-01-02T10:00:00Z"

    def test_default_properties_factory(self):
        """Test that properties has default factory."""
        api_data1 = WarehouseAPIData(
            id="warehouse-1",
            workspaceId="workspace-1",
            displayName="Warehouse 1",
            description="Description 1",
            type="Warehouse"
        )

        api_data2 = WarehouseAPIData(
            id="warehouse-2",
            workspaceId="workspace-2",
            displayName="Warehouse 2",
            description="Description 2",
            type="Warehouse"
        )

        # Each instance should have its own properties object
        assert api_data1.properties is not api_data2.properties
        assert isinstance(api_data1.properties, WarehouseProperties)
        assert isinstance(api_data2.properties, WarehouseProperties)

    def test_required_attributes_exist(self):
        """Test that all required attributes exist."""
        api_data = WarehouseAPIData(
            id="test-id",
            workspaceId="workspace-123",
            displayName="Test Name",
            description="Test Description",
            type="Test Type"
        )

        # Required attributes from BaseItemAPIData
        assert hasattr(api_data, 'id')
        assert hasattr(api_data, 'displayName')
        assert hasattr(api_data, 'description')
        assert hasattr(api_data, 'type')

        # Additional attributes
        assert hasattr(api_data, 'properties')

    def test_warehouse_api_data_none_values(self):
        """Test WarehouseAPIData with None values for optional fields."""
        api_data = WarehouseAPIData(
            id="warehouse-123",
            displayName="Test Warehouse",
            description="Test Description",
            type="Warehouse",
            workspaceId=None,
            properties=None
        )

        assert api_data.workspaceId is None
        assert api_data.properties is None


class TestWarehouse:

    def authenticate(self) -> None:
        set_global_fabric_client(get_env_svc())

    def rand_warehouse(self, workspace_id: str, folder: WorkspaceFolder = None) -> Warehouse:
        name = rand_workspace_item_name("WH")
        return Warehouse(
            workspace_id=workspace_id,
            name=name,
            description="Test Warehouse",
            folder=folder
        )

    def test_init_warehouse(self, workspace_id: str):
        warehouse: Warehouse = self.rand_warehouse(workspace_id)
        assert warehouse.item.fields.get("displayName", "").startswith("WH_")
        assert warehouse.item.fields.get("description") == "Test Warehouse"

    def test_from_json(self, workspace_id: str):
        json_data = {
            "workspaceId": workspace_id,
            "displayName": "WP_Test",
            "description": "Test Warehouse from JSON",
            "id": "12345",
            "type": "Warehouse",
            "properties": {
                "connectionInfo": "<ConnectionInfo>",
                "connectionString": "<ConnectionString>",
                "createdDate": "<CreatedDate>",
                "creationMode": "<CreationMode>",
                "sourceRestorePoint": "<SourceRestorePoint>",
                "collationType": "<CollationType>",
                "lastUpdatedTime": "<LastUpdatedTime>"
            }
        }
        obj = Warehouse.from_json(json_data)
        assert obj.item.fields.get("displayName") == json_data["displayName"]
        assert obj.item.fields.get("description") == json_data["description"]
        assert obj.item.api.displayName == json_data["displayName"]
        assert obj.item.api.description == json_data["description"]
        assert obj.item.api.id == json_data["id"]
        assert obj.item.api.type == json_data["type"]
        assert obj.item.api.properties.connectionInfo == json_data["properties"]["connectionInfo"]
        assert obj.item.api.properties.connectionString == json_data["properties"]["connectionString"]
        assert obj.item.api.properties.createdDate == json_data["properties"]["createdDate"]
        assert obj.item.api.properties.creationMode == json_data["properties"]["creationMode"]
        assert obj.item.api.properties.sourceRestorePoint == json_data["properties"]["sourceRestorePoint"]
        assert obj.item.api.properties.collationType == json_data["properties"]["collationType"]
        assert obj.item.api.properties.lastUpdatedTime == json_data["properties"]["lastUpdatedTime"]

    def test_create(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_warehouse(workspace_id)
        obj.create()
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "Warehouse"
        assert obj.item.api.properties.connectionInfo is not None
        assert obj.item.api.properties.connectionString is not None
        assert obj.item.api.properties.createdDate is not None
        assert obj.item.api.properties.creationMode is not None
        assert obj.item.api.properties.collationType is not None
        assert obj.item.api.properties.lastUpdatedTime is not None

    def test_create_in_folder(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        obj = self.rand_warehouse(workspace_id, folder=folder_singleton)
        obj.create()
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "Warehouse"

    def test_update(self, warehouse_singleton: Warehouse):
        self.authenticate()
        assert warehouse_singleton.item.api.description == "Test Warehouse"
        warehouse_singleton.update(description="Updated Description")
        assert warehouse_singleton.item.api.description == "Updated Description"

    def test_fetch_and_delete(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_warehouse(workspace_id)
        obj.create()
        obj.fetch()
        obj.delete()
        with pytest.raises(requests.HTTPError):
            obj.fetch()

    def test_get_by_name(self, warehouse_singleton: Warehouse):
        self.authenticate()
        fetched_obj = Warehouse.get_by_name(
            warehouse_singleton.item.api.workspaceId,
            warehouse_singleton.item.api.displayName
        )
        assert fetched_obj.item.api.id == warehouse_singleton.item.api.id

    def test_get_by_id(self, warehouse_singleton: Warehouse):
        self.authenticate()
        fetched_obj = Warehouse.get_by_id(
            warehouse_singleton.item.api.workspaceId,
            warehouse_singleton.item.api.id
        )
        assert fetched_obj.item.api.id == warehouse_singleton.item.api.id

    def test_list(self, workspace_id: str):
        self.authenticate()
        warehouses = Warehouse.list(workspace_id)
        assert isinstance(warehouses, list)
        assert len(warehouses) > 0
        for obj in warehouses:
            assert isinstance(obj, Warehouse)
            assert obj.item.api.id is not None
            assert obj.item.api.displayName is not None
            assert obj.item.api.description is not None

    def test_exists(self, workspace_id: str, warehouse_singleton: Warehouse):
        self.authenticate()
        obj = self.rand_warehouse(workspace_id)
        assert not obj.exists()
        assert warehouse_singleton.exists()

    def test_create_if_not_exists(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_warehouse(workspace_id)
        assert not obj.exists()
        obj.create_if_not_exists()
        assert obj.exists()
        obj.create_if_not_exists()

    def test_fetch_definition(self, warehouse_singleton: Warehouse):
        self.authenticate()
        with pytest.raises(requests.HTTPError):
            warehouse_singleton.fetch_definition()
