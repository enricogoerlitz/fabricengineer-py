from dataclasses import dataclass

from fabricengineer.api.fabric.workspace.items.base import BaseWorkspaceItem, FabricItem


@dataclass
class WorkspaceFolderAPIData:
    id: str
    workspaceId: str
    displayName: str
    parentFolderId: str = None


ITEM_PATH = "/folders"


class WorkspaceFolder(BaseWorkspaceItem[WorkspaceFolderAPIData]):
    """
    REF: https://learn.microsoft.com/en-us/rest/api/fabric/?
    """
    def __init__(
        self,
        workspace_id: str,
        name: str,
        folder_id: str = None,
        api_data: WorkspaceFolderAPIData = None
    ):
        item = FabricItem[WorkspaceFolderAPIData](
            displayName=name,
            parentFolderId=folder_id,
            api_data=api_data
        )
        super().__init__(
            create_type_fn=WorkspaceFolder.from_json,
            base_item_url=ITEM_PATH,
            workspace_id=workspace_id,
            item=item
        )

    @staticmethod
    def from_json(item: dict) -> "WorkspaceFolder":
        kwargs = item.copy()
        api_data = WorkspaceFolderAPIData(**kwargs)
        return WorkspaceFolder(
            workspace_id=api_data.workspaceId,
            name=api_data.displayName,
            api_data=api_data
        )

    @staticmethod
    def get_by_name(workspace_id: str, name: str) -> "WorkspaceFolder":
        return BaseWorkspaceItem.get_by_name(
            create_item_type_fn=WorkspaceFolder.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            name=name
        )

    @staticmethod
    def get_by_id(workspace_id: str, id: str) -> "WorkspaceFolder":
        return BaseWorkspaceItem.get_by_id(
            create_item_type_fn=WorkspaceFolder.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            id=id
        )

    @staticmethod
    def list(workspace_id: str) -> list[WorkspaceFolderAPIData]:
        return [
            WorkspaceFolder.from_json(item)
            for item in BaseWorkspaceItem.list(
                workspace_id=workspace_id,
                base_item_url=ITEM_PATH
            )
        ]
