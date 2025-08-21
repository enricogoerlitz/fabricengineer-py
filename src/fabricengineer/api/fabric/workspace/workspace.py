from dataclasses import dataclass, field
from typing import Optional

from fabricengineer.api.fabric.workspace.base import BaseWorkspaceItem, FabricItem


ITEM_PATH = ""


@dataclass
class WorkspaceOneLakeEndpoints:
    blobEndpoint: Optional[str] = None
    dfsEndpoint: Optional[str] = None


@dataclass
class WorkspaceAPIData:
    id: str
    displayName: str
    description: str
    type: str
    capacityId: Optional[str] = None
    capacityRegion: Optional[str] = None
    capacityAssignmentProgress: Optional[str] = None
    oneLakeEndpoints: Optional[WorkspaceOneLakeEndpoints] = field(default_factory=WorkspaceOneLakeEndpoints)


class Workspace(BaseWorkspaceItem[WorkspaceAPIData]):
    def __init__(
        self,
        name: str,
        description: str = None,
        capacity_id: str = None,
        api_data: WorkspaceAPIData = None
    ):
        description = description or "New Workspace"
        item = FabricItem[WorkspaceAPIData](
            displayName=name,
            description=description,
            capacityId=capacity_id,
            api_data=api_data
        )
        super().__init__(
            create_type_fn=Workspace.from_json,
            base_item_url=ITEM_PATH,
            item=item
        )

    @staticmethod
    def from_json(item: dict) -> "Workspace":
        kwargs = item.copy()
        if "oneLakeEndpoints" not in item.keys():
            item["oneLakeEndpoints"] = {}
        kwargs["oneLakeEndpoints"] = WorkspaceOneLakeEndpoints(**item["oneLakeEndpoints"])
        api_data = WorkspaceAPIData(**kwargs)
        return Workspace(
            name=api_data.displayName,
            description=api_data.description,
            api_data=api_data
        )

    @staticmethod
    def get_by_name(name: str) -> "Workspace":
        return BaseWorkspaceItem.get_by_name(
            create_item_type_fn=Workspace.from_json,
            workspace_id=None,
            base_item_url=ITEM_PATH,
            name=name
        )

    @staticmethod
    def get_by_id(id: str) -> "Workspace":
        return BaseWorkspaceItem.get_by_id(
            create_item_type_fn=Workspace.from_json,
            workspace_id=None,
            base_item_url=ITEM_PATH,
            id=id
        )

    @staticmethod
    def list() -> list["Workspace"]:
        return [
            Workspace.from_json(item)
            for item in BaseWorkspaceItem.list(
                workspace_id=None,
                base_item_url=ITEM_PATH
            )
        ]
