from dataclasses import dataclass, field
from typing import Optional

from fabricengineer.api.fabric.workspace.base import (
    BaseWorkspaceItem,
    BaseItemAPIData,
    FabricItem,
    WorkspaceItemDependency,
    WorkspaceItemDependencyType
)
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder


ITEM_PATH = "/warehouses"


@dataclass
class WarehouseProperties:
    connectionInfo: Optional[str] = None
    connectionString: Optional[str] = None
    createdDate: Optional[str] = None
    creationMode: Optional[str] = None
    sourceRestorePoint: Optional[str] = None
    collationType: Optional[str] = None
    lastUpdatedTime: Optional[str] = None


@dataclass
class WarehouseAPIData(BaseItemAPIData):
    folderId: str = None
    properties: WarehouseProperties = field(default_factory=WarehouseProperties)


class Warehouse(BaseWorkspaceItem[WarehouseAPIData]):
    """
    REF: https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/items
    """
    def __init__(
        self,
        workspace_id: str,
        name: str,
        description: str = None,
        folder: str = None,
        collation_type: str = "Latin1_General_100_BIN2_UTF8",  # Case-sensitive
        api_data: WarehouseAPIData = None
    ):
        depends_on = None
        folder = None if not isinstance(folder, WorkspaceFolder) else folder
        if folder is not None:
            depends_on = [
                WorkspaceItemDependency(folder, WorkspaceItemDependencyType.FOLDER, "folderId")
            ]
        description = description or "New Warehouse"
        item = FabricItem[WarehouseAPIData](
            displayName=name,
            description=description,
            folderId=folder,
            creationPayload={
                "collationType": collation_type
            },
            api_data=api_data
        )
        super().__init__(
            create_type_fn=Warehouse.from_json,
            base_item_url=ITEM_PATH,
            workspace_id=workspace_id,
            item=item,
            depends_on=depends_on
        )

    @staticmethod
    def from_json(item: dict) -> "Warehouse":
        kwargs = item.copy()
        if "properties" not in item.keys():
            item["properties"] = {}
        kwargs["properties"] = WarehouseProperties(**item["properties"])
        api_data = WarehouseAPIData(**kwargs)
        return Warehouse(
            workspace_id=api_data.workspaceId,
            name=api_data.displayName,
            description=api_data.description,
            api_data=api_data
        )

    @staticmethod
    def get_by_name(workspace_id: str, name: str) -> "Warehouse":
        return BaseWorkspaceItem.get_by_name(
            create_item_type_fn=Warehouse.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            name=name
        )

    @staticmethod
    def get_by_id(workspace_id: str, id: str) -> "Warehouse":
        return BaseWorkspaceItem.get_by_id(
            create_item_type_fn=Warehouse.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            id=id
        )

    @staticmethod
    def list(workspace_id: str) -> list["Warehouse"]:
        return [
            Warehouse.from_json(item)
            for item in BaseWorkspaceItem.list(
                workspace_id=workspace_id,
                base_item_url=ITEM_PATH
            )
        ]
