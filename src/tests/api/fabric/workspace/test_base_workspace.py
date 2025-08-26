import pytest

from fabricengineer.api.fabric.workspace.base import (
    BaseItemAPIData,
    FabricItem,
    ItemDefinitionInterface,
    CopyItemDefinition
)


class TestBaseItemAPIData:
    def test_base_item_api_data_creation(self):
        data = BaseItemAPIData(
            id="test-id",
            workspaceId="workspace-123",
            displayName="Test Item",
            description="Test description",
            type="TestType"
        )
        assert data.id == "test-id"
        assert data.workspaceId == "workspace-123"
        assert data.displayName == "Test Item"
        assert data.description == "Test description"
        assert data.type == "TestType"


class TestFabricItem:
    def test_fabric_item_creation_with_api_data(self):
        api_data = BaseItemAPIData(
            id="test-id",
            workspaceId="workspace-123",
            displayName="Test Item",
            description="Test description",
            type="TestType"
        )
        item = FabricItem(api_data=api_data, displayName="Test Item")
        assert item.api == api_data
        assert item.fields["displayName"] == "Test Item"

    def test_fabric_item_creation_without_api_data(self):
        item = FabricItem(displayName="Test Item", type="TestType")
        assert item.api is None
        assert item.fields["displayName"] == "Test Item"
        assert item.fields["type"] == "TestType"

    def test_fabric_item_creation_with_fields_only(self):
        item = FabricItem(name="test", value=42, active=True)
        assert item.api is None
        assert item.fields["name"] == "test"
        assert item.fields["value"] == 42
        assert item.fields["active"] is True


class TestItemDefinitionInterface:
    def test_item_definition_interface_is_abstract(self):
        with pytest.raises(TypeError):
            ItemDefinitionInterface()

    def test_concrete_implementation_must_implement_get_definition(self):
        class ConcreteImplementation(ItemDefinitionInterface):
            pass

        with pytest.raises(TypeError):
            ConcreteImplementation()

    def test_valid_concrete_implementation(self):
        class ValidImplementation(ItemDefinitionInterface):
            def get_definition(self) -> dict:
                return {"test": "definition"}

        impl = ValidImplementation()
        assert impl.get_definition() == {"test": "definition"}


class TestCopyItemDefinition:
    def test_copy_item_definition_creation(self):
        definition = CopyItemDefinition(
            workspace_id="workspace-123",
            id="item-456",
            item_uri_name="notebooks"
        )
        assert definition._wid == "workspace-123"
        assert definition._id == "item-456"
        assert definition._item_uri_name == "notebooks"

    def test_copy_item_definition_has_get_definition_method(self):
        definition = CopyItemDefinition(
            workspace_id="workspace-123",
            id="item-456",
            item_uri_name="notebooks"
        )
        assert hasattr(definition, 'get_definition')
        assert callable(getattr(definition, 'get_definition'))

    def test_copy_item_definition_implements_interface(self):
        definition = CopyItemDefinition(
            workspace_id="workspace-123",
            id="item-456",
            item_uri_name="notebooks"
        )
        assert isinstance(definition, ItemDefinitionInterface)
