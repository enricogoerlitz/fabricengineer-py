from dataclasses import dataclass

from fabricengineer.api.fabric.workspace.base import (
    BaseWorkspaceItem,
    FabricItem,
    WorkspaceItemDependency,
    WorkspaceItemDependencyType
)


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
        parent_folder: "WorkspaceFolder" = None,
        api_data: WorkspaceFolderAPIData = None
    ):
        depends_on = None
        parent_folder = None if not isinstance(parent_folder, WorkspaceFolder) else parent_folder
        if parent_folder is not None:
            depends_on = [
                WorkspaceItemDependency(parent_folder, WorkspaceItemDependencyType.FOLDER, "parentFolderId")
            ]
        item = FabricItem[WorkspaceFolderAPIData](
            displayName=name,
            parentFolderId=parent_folder,
            api_data=api_data
        )
        super().__init__(
            create_type_fn=WorkspaceFolder.from_json,
            base_item_url=ITEM_PATH,
            workspace_id=workspace_id,
            item=item,
            depends_on=depends_on
        )

    @staticmethod
    def from_json(item: dict) -> "WorkspaceFolder":
        kwargs = item.copy()
        api_data = WorkspaceFolderAPIData(**kwargs)
        return WorkspaceFolder(
            workspace_id=api_data.workspaceId,
            name=api_data.displayName,
            parent_folder=api_data.parentFolderId,
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
    def list(workspace_id: str) -> list["WorkspaceFolder"]:
        return [
            WorkspaceFolder.from_json(item)
            for item in BaseWorkspaceItem.list(
                workspace_id=workspace_id,
                base_item_url=ITEM_PATH
            )
        ]
