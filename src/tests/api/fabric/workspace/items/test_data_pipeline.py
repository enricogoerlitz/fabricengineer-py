import pytest
import requests

from fabricengineer.api.fabric.client.fabric import set_global_fabric_client, get_env_svc
from fabricengineer.api.fabric.workspace.items.data_pipeline import (
    DataPipeline,
    DataPipelineAPIData,
    CopyDataPipelineDefinition,
    ZIPDataPipelineDefinition
)
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from tests.utils import rand_workspace_item_name


@pytest.fixture
def data_pipeline_singleton(workspace_id: str) -> DataPipeline:
    """Fixture to create a DataPipeline instance."""
    set_global_fabric_client(get_env_svc())
    name = rand_workspace_item_name("DP")
    data_pipeline = DataPipeline(
        workspace_id=workspace_id,
        name=name,
        description="Test DataPipeline"
    )
    data_pipeline.create()
    return data_pipeline


class TestDataPipelineAPIData:
    """Test DataPipelineAPIData dataclass."""

    def test_create_with_required_fields(self):
        """Test creating DataPipelineAPIData with only required fields."""
        api_data = DataPipelineAPIData(
            id="datapipeline-123",
            workspaceId="workspace-123",
            displayName="Test DataPipeline",
            description="Test Description",
            type="DataPipeline"
        )

        assert api_data.id == "datapipeline-123"
        assert api_data.displayName == "Test DataPipeline"
        assert api_data.description == "Test Description"
        assert api_data.type == "DataPipeline"

    def test_required_attributes_exist(self):
        """Test that all required attributes exist."""
        api_data = DataPipelineAPIData(
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


class TestDataPipeline:
    test_dp: DataPipeline = None

    def authenticate(self) -> None:
        set_global_fabric_client(get_env_svc())

    def rand_data_pipeline(self, workspace_id: str, folder: WorkspaceFolder = None) -> DataPipeline:
        name = rand_workspace_item_name("DP")
        return DataPipeline(
            workspace_id=workspace_id,
            name=name,
            description="Test DataPipeline",
            folder=folder
        )

    def test_init_data_pipeline(self, workspace_id: str):
        data_pipeline: DataPipeline = self.rand_data_pipeline(workspace_id)
        assert data_pipeline.item.fields.get("displayName", "").startswith("DP_")
        assert data_pipeline.item.fields.get("description") == "Test DataPipeline"

    def test_from_json(self, workspace_id: str):
        json_data = {
            "workspaceId": workspace_id,
            "displayName": "DP_Test",
            "description": "Test DataPipeline from JSON",
            "id": "12345",
            "type": "DataPipeline"
        }
        obj = DataPipeline.from_json(json_data)
        assert obj.item.fields.get("displayName") == json_data["displayName"]
        assert obj.item.fields.get("description") == json_data["description"]
        assert obj.item.api.displayName == json_data["displayName"]
        assert obj.item.api.description == json_data["description"]
        assert obj.item.api.id == json_data["id"]
        assert obj.item.api.type == json_data["type"]

    def test_create_without_definition(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_data_pipeline(workspace_id)
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "DataPipeline"

    def test_create_with_zip(self, workspace_id: str):
        self.authenticate()
        path = "./src/tests/data/pipelines/TEST_PIPELINE.zip"
        definition = ZIPDataPipelineDefinition(zip_path=path)
        name = rand_workspace_item_name("DP")
        obj = DataPipeline(
            workspace_id=workspace_id,
            name=name,
            description="Test DataPipeline",
            definition=definition
        )
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "DataPipeline"
        assert obj.fetch_definition() is not None

    def test_create_with_copy_fabric_data_pipeline(self, data_pipeline_singleton: DataPipeline):
        self.authenticate()
        obj_template = data_pipeline_singleton
        definition = CopyDataPipelineDefinition(
            workspace_id=obj_template.item.api.workspaceId,
            pipeline_id=obj_template.item.api.id
        )
        name = rand_workspace_item_name("DP")
        obj = DataPipeline(
            workspace_id=obj_template.item.api.workspaceId,
            name=name,
            description="Test DataPipeline",
            definition=definition
        )
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "DataPipeline"
        assert obj.fetch_definition() is not None

    def test_create_in_folder(self, workspace_id: str, folder_singleton: WorkspaceFolder):
        self.authenticate()
        obj = self.rand_data_pipeline(workspace_id, folder_singleton)
        obj.create(max_retry_seconds_at_202=1)
        assert obj.item.api.id is not None
        assert obj.item.api.displayName == obj.item.fields.get("displayName")
        assert obj.item.api.description == obj.item.fields.get("description")
        assert obj.item.api.type == "DataPipeline"

    def test_update(self, data_pipeline_singleton: DataPipeline):
        self.authenticate()
        assert data_pipeline_singleton.item.api.description == "Test DataPipeline"
        data_pipeline_singleton.update(description="Updated Description")
        assert data_pipeline_singleton.item.api.description == "Updated Description"

    def test_fetch_and_delete(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_data_pipeline(workspace_id)
        obj.create()
        obj.fetch()
        obj.delete()
        with pytest.raises(requests.HTTPError):
            obj.fetch()

    def test_get_by_name(self, workspace_id: str, data_pipeline_singleton: DataPipeline):
        self.authenticate()
        fetched_obj = DataPipeline.get_by_name(
            workspace_id,
            data_pipeline_singleton.item.api.displayName
        )
        assert fetched_obj.item.api.id == data_pipeline_singleton.item.api.id

    def test_get_by_id(self, workspace_id: str, data_pipeline_singleton: DataPipeline):
        self.authenticate()
        fetched_obj = DataPipeline.get_by_id(workspace_id, data_pipeline_singleton.item.api.id)
        assert fetched_obj.item.api.id == data_pipeline_singleton.item.api.id

    def test_list(self, workspace_id: str):
        self.authenticate()
        data_pipelines = DataPipeline.list(workspace_id)
        assert isinstance(data_pipelines, list)
        assert len(data_pipelines) > 0
        for obj in data_pipelines:
            assert isinstance(obj, DataPipeline)
            assert obj.item.api.id is not None
            assert obj.item.api.displayName is not None
            assert obj.item.api.description is not None

    def test_exists(self, workspace_id: str, data_pipeline_singleton: DataPipeline):
        self.authenticate()
        obj = self.rand_data_pipeline(workspace_id)
        assert not obj.exists()
        assert data_pipeline_singleton.exists()

    def test_create_if_not_exists(self, workspace_id: str):
        self.authenticate()
        obj = self.rand_data_pipeline(workspace_id)
        assert not obj.exists()
        obj.create_if_not_exists()
        assert obj.exists()
        obj.create_if_not_exists()

    def test_fetch_definition(self, data_pipeline_singleton: DataPipeline):
        self.authenticate()
        definition = data_pipeline_singleton.fetch_definition()
        assert definition is not None
        assert isinstance(definition, dict)
