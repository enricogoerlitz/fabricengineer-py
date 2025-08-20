import json

from dataclasses import dataclass


from fabricengineer.api.utils import base64_encode
from fabricengineer.api.fabric.workspace.items.base import (
    BaseWorkspaceItem,
    BaseItemAPIData,
    FabricItem,
    ItemDefinitionInterface,
    CopyItemDefinition
)


ITEM_PATH = "/notebooks"


def read_ipynb_notebook(file_path: str) -> dict:
    with open(file_path, "r") as f:
        notebook_content = json.load(f)
    return notebook_content


@dataclass
class NotebookAPIData(BaseItemAPIData):
    pass


class CopyFabricNotebookDefinition(CopyItemDefinition):
    def __init__(self, workspace_id: str, id: str):
        super().__init__(
            workspace_id=workspace_id,
            id=id,
            item_uri_name="notebooks"
        )


class IPYNBNotebookDefinition(ItemDefinitionInterface):
    def __init__(self, code: str | dict):
        if isinstance(code, dict):
            code = json.dumps(code)
        self._code = code

    def get_definition(self) -> dict:
        code_b64 = base64_encode(self._code)
        platform_payload_b64 = base64_encode({
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "Notebook",
                "displayName": "notebook",
                "description": "New Notebook"
            },
            "config": {
                "version": "2.0",
                "logicalId": "00000000-0000-0000-0000-000000000000"
            }
        })

        return {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": code_b64,
                    "payloadType": "InlineBase64"
                },
                {
                    "path": ".platform",
                    "payload": platform_payload_b64,
                    "payloadType": "InlineBase64"
                }
            ]
        }


class Notebook(BaseWorkspaceItem[NotebookAPIData]):
    """
    REF: https://learn.microsoft.com/en-us/rest/api/fabric/notebook/items
    """
    def __init__(
        self,
        workspace_id: str,
        name: str,
        description: str = None,
        folder_id: str = None,
        definition: ItemDefinitionInterface = None,
        api_data: NotebookAPIData = None
    ):
        definition = definition.get_definition() if isinstance(definition, ItemDefinitionInterface) else None
        description = description or "New Notebook"
        item = FabricItem[NotebookAPIData](
            displayName=name,
            description=description,
            folderId=folder_id,
            definition=definition,
            apiData=api_data
        )
        super().__init__(
            create_type_fn=Notebook.from_json,
            base_item_url=ITEM_PATH,
            workspace_id=workspace_id,
            item=item
        )

    @staticmethod
    def from_json(item: dict) -> "Notebook":
        kwargs = item.copy()
        api_data = NotebookAPIData(**kwargs)
        return Notebook(
            workspace_id=api_data.workspaceId,
            name=api_data.displayName,
            description=api_data.description,
            api_data=api_data
        )

    @staticmethod
    def get_by_name(workspace_id: str, name: str) -> "Notebook":
        return BaseWorkspaceItem.get_by_name(
            create_fn=Notebook.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            name=name
        )

    @staticmethod
    def get_by_id(workspace_id: str, id: str) -> "Notebook":
        return BaseWorkspaceItem.get_by_id(
            create_fn=Notebook.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            id=id
        )

    @staticmethod
    def list(workspace_id: str) -> list["Notebook"]:
        return [
            Notebook.from_json(item)
            for item in BaseWorkspaceItem.list(
                workspace_id=workspace_id,
                base_item_url=ITEM_PATH
            )
        ]
