import json
import zipfile

from dataclasses import dataclass

from fabricengineer.api.fabric.workspace.base import (
    BaseWorkspaceItem,
    BaseItemAPIData,
    FabricItem,
    ItemDefinitionInterface,
    CopyItemDefinition,
    WorkspaceItemDependency,
    WorkspaceItemDependencyType
)
from fabricengineer.api.utils import base64_encode
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder


ITEM_PATH = "/dataPipelines"


def read_zip_pipeline_json(zip_filepath: str):
    with zipfile.ZipFile(zip_filepath, "r") as zf:
        pipeline_file = [
            file
            for file in zf.namelist()
            if file.endswith(".json") and "manifest.json" not in file
        ]
        assert len(pipeline_file) == 1, "Es sollte genau eine JSON-Datei im ZIP sein."
        pipeline_file = pipeline_file[0]
        with zf.open(pipeline_file) as f:
            pipeline_json = json.load(f)

    assert "resources" in pipeline_json, "Die JSON-Datei muss 'resources' enthalten."
    assert len(pipeline_json["resources"]) == 1, "Die JSON-Datei muss mindestens eine Ressource enthalten."
    resource = pipeline_json["resources"][0]
    assert "properties" in resource, "Die Ressource muss 'properties' enthalten."
    return {
        "properties": resource["properties"]
    }


@dataclass
class DataPipelineAPIData(BaseItemAPIData):
    folderId: str = None


class CopyDataPipelineDefinition(CopyItemDefinition):
    def __init__(self, workspace_id: str, pipeline_id: str):
        super().__init__(
            workspace_id=workspace_id,
            id=pipeline_id,
            item_uri_name="dataPipelines"
        )


class ZIPDataPipelineDefinition(ItemDefinitionInterface):
    def __init__(self, *, pipeline_json: str = None, zip_path: str = None):
        if pipeline_json is None and zip_path is None:
            raise ValueError("Either pipeline_json or zip_path must be provided")
        if isinstance(zip_path, str):
            pipeline_json = read_zip_pipeline_json(zip_path)
        self._pipeline_json = pipeline_json

    def get_definition(self) -> dict:
        pipeline_json_b64 = base64_encode(self._pipeline_json)
        platform_payload_b64 = base64_encode({
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "DataPipeline",
                "displayName": "pipelineX",
                "description": "A Description"
            },
            "config": {
                "version": "2.0",
                "logicalId": "00000000-0000-0000-0000-000000000000"
            }
        })

        return {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": pipeline_json_b64,
                    "payloadType": "InlineBase64"
                },
                {
                    "path": ".platform",
                    "payload": platform_payload_b64,
                    "payloadType": "InlineBase64"
                }
            ]
        }


class DataPipeline(BaseWorkspaceItem[DataPipelineAPIData]):
    """
    REF: https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items
    """
    def __init__(
        self,
        workspace_id: str,
        name: str,
        description: str = None,
        folder: WorkspaceFolder = None,
        definition: ItemDefinitionInterface = None,
        api_data: DataPipelineAPIData = None
    ):
        depends_on = None
        folder = None if not isinstance(folder, WorkspaceFolder) else folder
        if folder is not None:
            depends_on = [
                WorkspaceItemDependency(folder, WorkspaceItemDependencyType.FOLDER, "folderId")
            ]
        definition = definition.get_definition() if isinstance(definition, ItemDefinitionInterface) else None
        description = description or "New Data Pipeline"
        item = FabricItem[DataPipelineAPIData](
            displayName=name,
            description=description,
            folderId=folder,
            definition=definition,
            api_data=api_data
        )
        super().__init__(
            create_type_fn=DataPipeline.from_json,
            base_item_url=ITEM_PATH,
            workspace_id=workspace_id,
            item=item,
            depends_on=depends_on
        )

    @staticmethod
    def from_json(item: dict) -> "DataPipeline":
        kwargs = item.copy()
        api_data = DataPipelineAPIData(**kwargs)
        return DataPipeline(
            workspace_id=api_data.workspaceId,
            name=api_data.displayName,
            description=api_data.description,
            api_data=api_data
        )

    @staticmethod
    def get_by_name(workspace_id: str, name: str) -> "DataPipeline":
        return BaseWorkspaceItem.get_by_name(
            create_item_type_fn=DataPipeline.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            name=name
        )

    @staticmethod
    def get_by_id(workspace_id: str, id: str) -> "DataPipeline":
        return BaseWorkspaceItem.get_by_id(
            create_item_type_fn=DataPipeline.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            id=id
        )

    @staticmethod
    def list(workspace_id: str) -> list["DataPipeline"]:
        return [
            DataPipeline.from_json(item)
            for item in BaseWorkspaceItem.list(
                workspace_id=workspace_id,
                base_item_url=ITEM_PATH
            )
        ]
