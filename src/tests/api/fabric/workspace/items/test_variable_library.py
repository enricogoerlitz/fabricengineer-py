import pytest
import requests

from fabricengineer.api.fabric.client.fabric import set_global_fabric_client, get_env_svc
from fabricengineer.api.fabric.workspace.items.variable_library import (
    VariableLibrary,
    VariableLibraryAPIData,
    VariableLibraryProperties,
    VariableLibraryDefinition,
    VariableLibraryVariable
)
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from tests.utils import rand_workspace_item_name


@pytest.fixture
def varlib_singleton(workspace_id: str):
    """Fixture to create a VariableLibrary instance."""
    set_global_fabric_client(get_env_svc())
    name = rand_workspace_item_name("VL")
    varlib = VariableLibrary(
        workspace_id=workspace_id,
        name=name,
        description="Test VariableLibrary"
    )
    varlib.create()
    return varlib


class TestVariableLibraryProperties:
    """Test VariableLibraryProperties dataclass."""

    def test_create_with_default_values(self):
        """Test creating VariableLibraryProperties with default values."""
        properties = VariableLibraryProperties()

        assert properties.activeValueSetName is None

    def test_create_with_values(self):
        """Test creating VariableLibraryProperties with specific values."""
        active_value_set_name = "test-active-value-set"
        properties = VariableLibraryProperties(
            activeValueSetName=active_value_set_name
        )

        assert properties.activeValueSetName == active_value_set_name

    def test_attributes_exist(self):
        """Test that all required attributes exist."""
        properties = VariableLibraryProperties()

        assert hasattr(properties, 'activeValueSetName')


class TestVariableLibraryAPIData:
    """Test VariableLibraryAPIData dataclass."""

    def test_create_with_required_fields(self):
        """Test creating VariableLibraryAPIData with only required fields."""
        api_data = VariableLibraryAPIData(
            id="variable-library-123",
            workspaceId="workspace-123",
            displayName="Test VariableLibrary",
            description="Test Description",
            type="VariableLibrary"
        )

        assert api_data.id == "variable-library-123"
        assert api_data.displayName == "Test VariableLibrary"
        assert api_data.description == "Test Description"
        assert api_data.type == "VariableLibrary"
        assert isinstance(api_data.properties, VariableLibraryProperties)

    def test_create_with_all_fields(self):
        """Test creating VariableLibraryAPIData with all fields."""
        properties = VariableLibraryProperties(
            activeValueSetName="test-active-value-set"
        )

        api_data = VariableLibraryAPIData(
            id="variable_library-456",
            displayName="Full Test VariableLibrary",
            description="Full test description",
            type="VariableLibrary",
            workspaceId="workspace-123",
            properties=properties
        )

        assert api_data.id == "variable_library-456"
        assert api_data.displayName == "Full Test VariableLibrary"
        assert api_data.description == "Full test description"
        assert api_data.type == "VariableLibrary"
        assert api_data.workspaceId == "workspace-123"
        assert api_data.properties == properties
        assert api_data.properties.activeValueSetName == "test-active-value-set"

    def test_default_properties_factory(self):
        """Test that properties has default factory."""
        api_data1 = VariableLibraryAPIData(
            id="variable_library-1",
            workspaceId="workspace-1",
            displayName="VariableLibrary 1",
            description="Description 1",
            type="VariableLibrary"
        )

        api_data2 = VariableLibraryAPIData(
            id="variable_library-2",
            workspaceId="workspace-2",
            displayName="VariableLibrary 2",
            description="Description 2",
            type="VariableLibrary"
        )

        # Each instance should have its own properties object
        assert api_data1.properties is not api_data2.properties
        assert isinstance(api_data1.properties, VariableLibraryProperties)
        assert isinstance(api_data2.properties, VariableLibraryProperties)

    def test_required_attributes_exist(self):
        """Test that all required attributes exist."""
        api_data = VariableLibraryAPIData(
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

    def test_variable_library_api_data_none_values(self):
        """Test VariableLibraryAPIData with None values for optional fields."""
        api_data = VariableLibraryAPIData(
            id="variable_library-123",
            displayName="Test VariableLibrary",
            description="Test Description",
            type="VariableLibrary",
            workspaceId=None,
            properties=None
        )

        assert api_data.workspaceId is None
        assert api_data.properties is None


class TestVariableLibrary:
    test_vl: VariableLibrary = None

    def authenticate(self) -> None:
        set_global_fabric_client(get_env_svc())

    def rand_variable_library(self, workspace_id: str, folder: WorkspaceFolder = None) -> VariableLibrary:
        name = rand_workspace_item_name("VL")
        return VariableLibrary(
            workspace_id=workspace_id,
            name=name,
            description="Test VariableLibrary",
            folder=folder
        )

    def test_init_variable_library(self, workspace_id: str):
        variable_library: VariableLibrary = self.rand_variable_library(workspace_id)
        assert variable_library.item.fields.get("displayName", "").startswith("VL_")
        assert variable_library.item.fields.get("description") == "Test VariableLibrary"

    def test_from_json(self, workspace_id: str):
        json_data = {
            "workspaceId": workspace_id,
            "displayName": "VL_Test",
            "description": "Test VariableLibrary from JSON",
            "id": "12345",
            "type": "VariableLibrary",
            "properties": {
                "activeValueSetName": "<ActiveValueSetName>"
            }
        }
        obj = VariableLibrary.from_json(json_data)
        assert obj.item.fields.get("displayName") == json_data["displayName"]
        assert obj.item.fields.get("description") == json_data["description"]
        assert obj.item.api.displayName == json_data["displayName"]
        assert obj.item.api.description == json_data["description"]
        assert obj.item.api.id == json_data["id"]
        assert obj.item.api.type == json_data["type"]
        assert obj.item.api.properties.activeValueSetName == json_data["properties"]["activeValueSetName"]

    def test_create(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_variable_library(workspace_id)
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "VariableLibrary"
        assert obj.item.api.properties.activeValueSetName is not None

    def test_create_in_folder(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        obj = self.rand_variable_library(workspace_id, folder_singleton)
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "VariableLibrary"
        assert obj.item.api.properties.activeValueSetName is not None

    def test_create_with_definition(self, workspace_id: str):
        self.authenticate()
        name = rand_workspace_item_name("VL")
        obj = VariableLibrary(
            workspace_id=workspace_id,
            name=name,
            description="Test VariableLibrary",
            definition=VariableLibraryDefinition(
                ["TEST", "PROD"],
                VariableLibraryVariable(
                    name="TestVariable1",
                    note="A test variable",
                    type="String",
                    value="TestValue"
                ),
                VariableLibraryVariable(
                    name="TestVariable2",
                    note="A test variable",
                    type="Integer",
                    value=100
                )
            )
        )
        obj.create(max_retry_seconds_at_202=1)
        definition = obj.fetch_definition()
        assert definition is not None
        assert isinstance(definition, dict)
        assert "definition" in definition.keys()

    def test_update(self, varlib_singleton: VariableLibrary):
        self.authenticate()
        assert varlib_singleton.item.api.description == "Test VariableLibrary"
        varlib_singleton.update(description="Updated Description")
        assert varlib_singleton.item.api.description == "Updated Description"

    def test_fetch_and_delete(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_variable_library(workspace_id)
        obj.create(max_retry_seconds_at_202=1)
        obj.fetch()
        obj.delete()
        with pytest.raises(requests.HTTPError):
            obj.fetch()

    def test_get_by_name(self, varlib_singleton: VariableLibrary):
        self.authenticate()
        fetched_obj = VariableLibrary.get_by_name(
            varlib_singleton.item.api.workspaceId,
            varlib_singleton.item.api.displayName
        )
        assert fetched_obj.item.api.id == varlib_singleton.item.api.id

    def test_get_by_id(self, varlib_singleton: VariableLibrary):
        self.authenticate()
        fetched_obj = VariableLibrary.get_by_id(
            varlib_singleton.item.api.workspaceId,
            varlib_singleton.item.api.id
        )
        assert fetched_obj.item.api.id == varlib_singleton.item.api.id

    def test_list(self, workspace_id: str):
        self.authenticate()
        variable_librarys = VariableLibrary.list(workspace_id)
        assert isinstance(variable_librarys, list)
        assert len(variable_librarys) > 0
        for obj in variable_librarys:
            assert isinstance(obj, VariableLibrary)
            assert obj.item.api.id is not None
            assert obj.item.api.displayName is not None
            assert obj.item.api.description is not None

    def test_exists(self, workspace_id: str, varlib_singleton: VariableLibrary):
        self.authenticate()
        obj = self.rand_variable_library(workspace_id)
        assert not obj.exists()
        assert varlib_singleton.exists()

    def test_create_if_not_exists(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_variable_library(workspace_id)
        assert not obj.exists()
        obj.create_if_not_exists()
        assert obj.exists()
        obj.create_if_not_exists()

    def test_fetch_definition(self, varlib_singleton: VariableLibrary):
        self.authenticate()
        definition = varlib_singleton.fetch_definition()
        assert definition is not None
        assert isinstance(definition, dict)
