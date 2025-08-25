import pytest
import requests

from fabricengineer.api.fabric.client.fabric import set_global_fabric_client, get_env_svc
from fabricengineer.api.fabric.workspace.items.notebook import (
    Notebook,
    NotebookAPIData,
    CopyFabricNotebookDefinition,
    IPYNBNotebookDefinition
)
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from tests.utils import rand_workspace_item_name


@pytest.fixture
def notebook_singleton(workspace_id: str):
    """Fixture to create a Notebook instance."""
    set_global_fabric_client(get_env_svc())
    name = rand_workspace_item_name("NB")
    notebook = Notebook(
        workspace_id=workspace_id,
        name=name,
        description="Test Notebook"
    )
    notebook.create()
    return notebook


class TestNotebookAPIData:
    """Test NotebookAPIData dataclass."""

    def test_create_with_required_fields(self):
        """Test creating NotebookAPIData with only required fields."""
        api_data = NotebookAPIData(
            id="datapipeline-123",
            workspaceId="workspace-123",
            displayName="Test Notebook",
            description="Test Description",
            type="Notebook"
        )

        assert api_data.id == "datapipeline-123"
        assert api_data.displayName == "Test Notebook"
        assert api_data.description == "Test Description"
        assert api_data.type == "Notebook"

    def test_required_attributes_exist(self):
        """Test that all required attributes exist."""
        api_data = NotebookAPIData(
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


class TestNotebook:
    test_nb: Notebook = None

    def authenticate(self) -> None:
        set_global_fabric_client(get_env_svc())

    def rand_notebook(self, workspace_id: str, folder: WorkspaceFolder = None) -> Notebook:
        name = rand_workspace_item_name("NB")
        return Notebook(
            workspace_id=workspace_id,
            name=name,
            description="Test Notebook",
            folder=folder
        )

    def test_init_notebook(self, workspace_id: str):
        notebook: Notebook = self.rand_notebook(workspace_id)
        assert notebook.item.fields.get("displayName", "").startswith("NB_")
        assert notebook.item.fields.get("description") == "Test Notebook"

    def test_from_json(self, workspace_id: str):
        json_data = {
            "workspaceId": workspace_id,
            "displayName": "NB_Test",
            "description": "Test Notebook from JSON",
            "id": "12345",
            "type": "Notebook"
        }
        obj = Notebook.from_json(json_data)
        assert obj.item.fields.get("displayName") == json_data["displayName"]
        assert obj.item.fields.get("description") == json_data["description"]
        assert obj.item.api.displayName == json_data["displayName"]
        assert obj.item.api.description == json_data["description"]
        assert obj.item.api.id == json_data["id"]
        assert obj.item.api.type == json_data["type"]

    def test_create_without_definition(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_notebook(workspace_id)
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "Notebook"

    def test_create_in_folder(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        obj = self.rand_notebook(workspace_id, folder_singleton)
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "Notebook"

    def test_create_with_ipynb(self, workspace_id: str):
        self.authenticate()
        path = "./src/tests/data/notebooks/TEST_NOTEBOOK.ipynb"
        definition = IPYNBNotebookDefinition(ipynb_path=path)
        name = rand_workspace_item_name("NB")
        obj = Notebook(
            workspace_id=workspace_id,
            name=name,
            description="Test Notebook",
            definition=definition
        )
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "Notebook"
        assert obj.fetch_definition() is not None

    def test_create_with_copy_fabric_notebook(self, workspace_id: str, notebook_singleton: Notebook):
        self.authenticate()
        obj_template = notebook_singleton
        definition = CopyFabricNotebookDefinition(
            workspace_id=workspace_id,
            notebook_id=obj_template.item.api.id
        )
        name = rand_workspace_item_name("NB")
        obj = Notebook(
            workspace_id=workspace_id,
            name=name,
            description="Test Notebook",
            definition=definition
        )
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "Notebook"
        assert obj.fetch_definition() is not None

    def test_update(self, notebook_singleton: Notebook):
        self.authenticate()
        assert notebook_singleton.item.api.description == "Test Notebook"
        notebook_singleton.update(description="Updated Description")
        assert notebook_singleton.item.api.description == "Updated Description"

    def test_fetch_and_delete(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_notebook(workspace_id)
        obj.create()
        obj.fetch()
        obj.delete()
        with pytest.raises(requests.HTTPError):
            obj.fetch()

    def test_get_by_name(self, notebook_singleton: Notebook):
        self.authenticate()
        fetched_obj = Notebook.get_by_name(
            notebook_singleton.item.api.workspaceId,
            notebook_singleton.item.api.displayName
        )
        assert fetched_obj.item.api.id == notebook_singleton.item.api.id

    def test_get_by_id(self, workspace_id: str, notebook_singleton: Notebook):
        self.authenticate()
        fetched_obj = Notebook.get_by_id(notebook_singleton.item.api.workspaceId, notebook_singleton.item.api.id)
        assert fetched_obj.item.api.id == notebook_singleton.item.api.id

    def test_list(self, workspace_id: str):
        self.authenticate()
        notebooks = Notebook.list(workspace_id)
        assert isinstance(notebooks, list)
        assert len(notebooks) > 0
        for obj in notebooks:
            assert isinstance(obj, Notebook)
            assert obj.item.api.id is not None
            assert obj.item.api.displayName is not None
            assert obj.item.api.description is not None

    def test_exists(self, workspace_id: str, notebook_singleton: Notebook):
        self.authenticate()
        obj = self.rand_notebook(workspace_id)
        assert not obj.exists()
        assert notebook_singleton.exists()

    def test_create_if_not_exists(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_notebook(workspace_id)
        assert not obj.exists()
        obj.create_if_not_exists()
        assert obj.exists()
        obj.create_if_not_exists()

    def test_fetch_definition(self, notebook_singleton: Notebook):
        self.authenticate()
        definition = notebook_singleton.fetch_definition()
        assert definition is not None
        assert isinstance(definition, dict)
