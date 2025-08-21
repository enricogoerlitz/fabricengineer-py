"""
Tests for BaseWorkspaceItem and related classes.

Run tests: uv run pytest src/tests/api/fabric/workspace/test_base_workspace.py -v
"""
import pytest
import requests
from unittest.mock import Mock, patch

from fabricengineer.api.fabric.workspace.base import (
    BaseItemAPIData,
    FabricItem,
    CopyItemDefinition,
    BaseWorkspaceItem,
    ItemDefinitionInterface
)


@pytest.fixture
def sample_api_data():
    """Create sample API data for testing."""
    return BaseItemAPIData(
        id="test-id-123",
        workspaceId="workspace-id-456",
        displayName="Test Item",
        description="Test Description",
        type="TestType"
    )


@pytest.fixture
def sample_fabric_item(sample_api_data):
    """Create a sample FabricItem for testing."""
    return FabricItem(
        api_data=sample_api_data,
        displayName="Test Item",
        description="Test Description",
        type="TestType"
    )


@pytest.fixture
def sample_fabric_item_no_api():
    """Create a sample FabricItem without API data for testing."""
    return FabricItem(
        displayName="Test Item",
        description="Test Description",
        type="TestType"
    )


class TestBaseItemAPIData:
    """Test BaseItemAPIData dataclass."""

    def test_create_base_item_api_data(self, sample_api_data):
        """Test creation of BaseItemAPIData."""
        assert sample_api_data.id == "test-id-123"
        assert sample_api_data.workspaceId == "workspace-id-456"
        assert sample_api_data.displayName == "Test Item"
        assert sample_api_data.description == "Test Description"
        assert sample_api_data.type == "TestType"

    def test_base_item_api_data_attributes_exist(self, sample_api_data):
        """Test that all required attributes exist."""
        assert hasattr(sample_api_data, 'id')
        assert hasattr(sample_api_data, 'workspaceId')
        assert hasattr(sample_api_data, 'displayName')
        assert hasattr(sample_api_data, 'description')
        assert hasattr(sample_api_data, 'type')


class TestFabricItem:
    """Test FabricItem class."""

    def test_create_fabric_item_with_api_data(self, sample_api_data):
        """Test creating FabricItem with API data."""
        item = FabricItem(
            api_data=sample_api_data,
            displayName="Test Item",
            description="Test Description"
        )

        assert item.api == sample_api_data
        assert item.fields["displayName"] == "Test Item"
        assert item.fields["description"] == "Test Description"

    def test_create_fabric_item_without_api_data(self):
        """Test creating FabricItem without API data."""
        item = FabricItem(
            displayName="Test Item",
            description="Test Description",
            type="TestType"
        )

        assert item.api is None
        assert item.fields["displayName"] == "Test Item"
        assert item.fields["description"] == "Test Description"
        assert item.fields["type"] == "TestType"

    def test_fabric_item_fields_are_dict(self, sample_fabric_item):
        """Test that fields are properly stored as dictionary."""
        assert isinstance(sample_fabric_item.fields, dict)
        assert "displayName" in sample_fabric_item.fields
        assert "description" in sample_fabric_item.fields

    def test_fabric_item_init_parameters(self):
        """Test FabricItem initialization with various parameters."""
        item = FabricItem(
            custom_field="custom_value",
            another_field=123,
            bool_field=True
        )

        assert item.fields["custom_field"] == "custom_value"
        assert item.fields["another_field"] == 123
        assert item.fields["bool_field"] is True
        assert item.api is None


class TestCopyItemDefinition:
    """Test CopyItemDefinition class."""

    def test_create_copy_item_definition(self):
        """Test creating CopyItemDefinition."""
        definition = CopyItemDefinition(
            workspace_id="workspace-123",
            id="item-456",
            item_uri_name="notebooks"
        )

        assert definition._wid == "workspace-123"
        assert definition._id == "item-456"
        assert definition._item_uri_name == "notebooks"

    def test_copy_item_definition_implements_interface(self):
        """Test that CopyItemDefinition implements ItemDefinitionInterface."""
        definition = CopyItemDefinition("ws", "id", "uri")
        assert isinstance(definition, ItemDefinitionInterface)
        assert hasattr(definition, 'get_definition')
        assert callable(definition.get_definition)

    @patch('fabricengineer.api.fabric.workspace.base.fabric_client')
    @patch('fabricengineer.api.fabric.workspace.base.http_wait_for_completion_after_202')
    def test_get_definition_call_structure(self, mock_wait, mock_client):
        """Test that get_definition makes correct API calls without actual execution."""
        # Setup mocks
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_client.post.return_value = mock_response
        mock_wait.return_value = {"definition": {"key": "value"}}

        definition = CopyItemDefinition("workspace-123", "item-456", "notebooks")

        # Test that get_definition method exists and can be called
        result = definition.get_definition()

        # Verify API call structure
        expected_url = "/workspaces/workspace-123/notebooks/item-456/getDefinition"
        mock_client.post.assert_called_once_with(expected_url, payload={})
        mock_response.raise_for_status.assert_called_once()
        mock_wait.assert_called_once_with(mock_response, retry_max_seconds=1)

        assert result == {"key": "value"}


class TestBaseWorkspaceItem:
    """Test BaseWorkspaceItem class."""

    def create_mock_item_type_fn(self, api_data):
        """Helper function to create mock item type function."""
        def mock_fn(data):
            mock_item = Mock()
            mock_item.item = Mock()
            mock_item.item.api = api_data
            return mock_item
        return mock_fn

    def test_create_base_workspace_item(self, sample_fabric_item, sample_api_data):
        """Test creating BaseWorkspaceItem."""
        create_fn = self.create_mock_item_type_fn(sample_api_data)

        item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/workspaces/test/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        assert item._create_item_type_fn == create_fn
        assert item._base_item_url == "/workspaces/test/notebooks"
        assert item._workspace_id == "workspace-123"
        assert item._item == sample_fabric_item

    def test_item_property(self, sample_fabric_item, sample_api_data):
        """Test item property getter."""
        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/workspaces/test/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        assert workspace_item.item == sample_fabric_item

    @patch('fabricengineer.api.fabric.workspace.base.fabric_client')
    def test_get_by_id_structure(self, mock_client, sample_api_data):
        """Test get_by_id method structure without actual API calls."""
        # Setup mock
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"id": "test-id", "displayName": "Test"}
        mock_client.workspaces.get.return_value = mock_response

        create_fn = self.create_mock_item_type_fn(sample_api_data)

        # Test method call structure
        result = BaseWorkspaceItem.get_by_id(
            create_item_type_fn=create_fn,
            workspace_id="workspace-123",
            base_item_url="/notebooks",
            id="item-456"
        )

        # Verify API call
        mock_client.workspaces.get.assert_called_once_with("workspace-123", "/notebooks/item-456")
        mock_response.raise_for_status.assert_called_once()
        assert result is not None

    @patch('fabricengineer.api.fabric.workspace.base.fabric_client')
    def test_get_by_name_structure(self, mock_client, sample_api_data):
        """Test get_by_name method structure without actual API calls."""
        # Setup mock
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "value": [
                {"displayName": "Other Item", "id": "other-id"},
                {"displayName": "Test Item", "id": "test-id"},
            ]
        }
        mock_client.workspaces.get.return_value = mock_response

        create_fn = self.create_mock_item_type_fn(sample_api_data)

        # Test method call structure
        result = BaseWorkspaceItem.get_by_name(
            create_item_type_fn=create_fn,
            workspace_id="workspace-123",
            base_item_url="/notebooks",
            name="Test Item"
        )

        # Verify API call
        mock_client.workspaces.get.assert_called_once_with("workspace-123", "/notebooks")
        mock_response.raise_for_status.assert_called_once()
        assert result is not None

    @patch('fabricengineer.api.fabric.workspace.base.fabric_client')
    def test_get_by_name_not_found(self, mock_client):
        """Test get_by_name when item is not found."""
        # Setup mock
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "value": [
                {"displayName": "Other Item", "id": "other-id"}
            ]
        }
        mock_client.workspaces.get.return_value = mock_response

        create_fn = Mock()

        # Test that exception is raised when item not found
        with pytest.raises(requests.HTTPError) as exc_info:
            BaseWorkspaceItem.get_by_name(
                create_item_type_fn=create_fn,
                workspace_id="workspace-123",
                base_item_url="/notebooks",
                name="Non-existent Item"
            )

        assert "404 Client Error: Item with displayName 'Non-existent Item' not found" in str(exc_info.value)

    @patch('fabricengineer.api.fabric.workspace.base.fabric_client')
    def test_list_structure(self, mock_client):
        """Test list method structure without actual API calls."""
        # Setup mock
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "value": [
                {"displayName": "Item 1", "id": "id-1"},
                {"displayName": "Item 2", "id": "id-2"},
            ]
        }
        mock_client.workspaces.get.return_value = mock_response

        # Test method call structure
        items = list(BaseWorkspaceItem.list("workspace-123", "/notebooks"))

        # Verify API call
        mock_client.workspaces.get.assert_called_once_with("workspace-123", "/notebooks")
        mock_response.raise_for_status.assert_called_once()

        assert len(items) == 2
        assert items[0]["displayName"] == "Item 1"
        assert items[1]["displayName"] == "Item 2"

    @patch('fabricengineer.api.fabric.workspace.base.BaseWorkspaceItem.get_by_name')
    def test_fetch_without_api_data(self, mock_get_by_name, sample_fabric_item_no_api, sample_api_data):
        """Test fetch method when item has no API data."""
        # Setup mock
        mock_result = Mock()
        mock_result.item.api = sample_api_data
        mock_get_by_name.return_value = mock_result

        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item_no_api,
            workspace_id="workspace-123"
        )

        result = workspace_item.fetch()

        # Verify method calls
        mock_get_by_name.assert_called_once()
        assert workspace_item._item.api == sample_api_data
        assert result == workspace_item

    @patch('fabricengineer.api.fabric.workspace.base.BaseWorkspaceItem.get_by_id')
    def test_fetch_with_api_data(self, mock_get_by_id, sample_fabric_item, sample_api_data):
        """Test fetch method when item has API data."""
        # Setup mock
        mock_result = Mock()
        mock_result.item.api = sample_api_data
        mock_get_by_id.return_value = mock_result

        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        result = workspace_item.fetch()

        # Verify method calls
        mock_get_by_id.assert_called_once_with(
            create_item_type_fn=create_fn,
            workspace_id="workspace-123",
            base_item_url="/notebooks",
            id=sample_api_data.id
        )
        assert result == workspace_item

    @patch('fabricengineer.api.fabric.workspace.base.fabric_client')
    def test_exists_true(self, mock_client, sample_fabric_item, sample_api_data):
        """Test exists method returns True when item exists."""
        # Setup mock for successful fetch
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"id": "test-id", "displayName": "Test"}
        mock_client.workspaces.get.return_value = mock_response

        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        result = workspace_item.exists()
        assert result is True

    @patch('fabricengineer.api.fabric.workspace.base.BaseWorkspaceItem.fetch')
    def test_exists_false(self, mock_fetch, sample_fabric_item, sample_api_data):
        """Test exists method returns False when item doesn't exist."""
        # Setup mock to raise 404 error
        mock_fetch.side_effect = requests.HTTPError("404 Client Error: Not found")

        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        result = workspace_item.exists()
        assert result is False

    @patch('fabricengineer.api.fabric.workspace.base.BaseWorkspaceItem.fetch')
    def test_exists_other_exception(self, mock_fetch, sample_fabric_item, sample_api_data):
        """Test exists method re-raises non-404 exceptions."""
        # Setup mock to raise other error
        mock_fetch.side_effect = requests.HTTPError("500 Server Error")

        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        with pytest.raises(requests.HTTPError) as exc_info:
            workspace_item.exists()

        assert "500 Server Error" in str(exc_info.value)

    def test_str_representation(self, sample_fabric_item, sample_api_data):
        """Test string representation of BaseWorkspaceItem."""
        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        str_repr = str(workspace_item)
        assert "BaseWorkspaceItem" in str_repr
        assert "workspaceId='workspace-123'" in str_repr
        assert "item=" in str_repr

    def test_str_representation_without_workspace_id(self, sample_fabric_item, sample_api_data):
        """Test string representation without workspace_id."""
        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item
        )

        str_repr = str(workspace_item)
        assert "BaseWorkspaceItem" in str_repr
        assert "workspaceId=" not in str_repr
        assert "item=" in str_repr

    def test_repr_equals_str(self, sample_fabric_item, sample_api_data):
        """Test that __repr__ equals __str__."""
        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        assert str(workspace_item) == repr(workspace_item)

    def test_retry_after_method(self, sample_fabric_item, sample_api_data):
        """Test _retry_after method."""
        create_fn = self.create_mock_item_type_fn(sample_api_data)

        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=sample_fabric_item,
            workspace_id="workspace-123"
        )

        # Test with Retry-After header
        mock_response = Mock()
        mock_response.headers = {"Retry-After": "10"}
        assert workspace_item._retry_after(mock_response) == 5  # Should be capped at 5

        # Test with lower Retry-After header
        mock_response.headers = {"Retry-After": "3"}
        assert workspace_item._retry_after(mock_response) == 3

        # Test without Retry-After header
        mock_response.headers = {}
        assert workspace_item._retry_after(mock_response) == 5  # Should default to 5


class TestItemDefinitionInterface:
    """Test ItemDefinitionInterface abstract base class."""

    def test_is_abstract_base_class(self):
        """Test that ItemDefinitionInterface is an abstract base class."""
        from abc import ABC
        assert issubclass(ItemDefinitionInterface, ABC)

    def test_cannot_instantiate_directly(self):
        """Test that ItemDefinitionInterface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ItemDefinitionInterface()

    def test_has_get_definition_method(self):
        """Test that ItemDefinitionInterface has abstract get_definition method."""
        assert hasattr(ItemDefinitionInterface, 'get_definition')
        assert ItemDefinitionInterface.get_definition.__isabstractmethod__


class TestIntegration:
    """Integration tests for the base classes."""

    def test_fabric_item_with_base_workspace_item(self, sample_api_data):
        """Test integration between FabricItem and BaseWorkspaceItem."""
        # Create FabricItem
        fabric_item = FabricItem(
            api_data=sample_api_data,
            displayName="Integration Test Item",
            description="Integration test description",
            type="Notebook"
        )

        # Create BaseWorkspaceItem
        create_fn = Mock()
        workspace_item = BaseWorkspaceItem(
            create_type_fn=create_fn,
            base_item_url="/notebooks",
            item=fabric_item,
            workspace_id="workspace-123"
        )

        # Test that everything is properly connected
        assert workspace_item.item.api.id == "test-id-123"
        assert workspace_item.item.fields["displayName"] == "Integration Test Item"
        assert workspace_item._workspace_id == "workspace-123"

    def test_copy_item_definition_integration(self):
        """Test CopyItemDefinition integration with ItemDefinitionInterface."""
        # Create CopyItemDefinition
        definition = CopyItemDefinition(
            workspace_id="workspace-123",
            id="item-456",
            item_uri_name="notebooks"
        )

        # Test that it properly implements the interface
        assert isinstance(definition, ItemDefinitionInterface)
        assert hasattr(definition, 'get_definition')
        assert callable(definition.get_definition)

        # Test internal state
        assert definition._wid == "workspace-123"
        assert definition._id == "item-456"
        assert definition._item_uri_name == "notebooks"
