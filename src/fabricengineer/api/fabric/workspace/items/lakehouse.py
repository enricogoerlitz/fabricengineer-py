import time

from dataclasses import dataclass, field
from typing import Optional

from fabricengineer.api.fabric.workspace.base import (
    BaseWorkspaceItem,
    BaseItemAPIData,
    FabricItem,
    WorkspaceItemDependency,
    WorkspaceItemDependencyType
)
from fabricengineer.logging.logger import logger
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder


ITEM_PATH = "/lakehouses"


@dataclass
class LakehouseSqlEndpointProperties:
    id: Optional[str] = None
    connectionString: Optional[str] = None
    provisioningStatus: Optional[str] = None


@dataclass
class LakehouseProperties:
    defaultSchema: Optional[str] = None
    oneLakeTablesPath: Optional[str] = None
    oneLakeFilesPath: Optional[str] = None
    sqlEndpointProperties: LakehouseSqlEndpointProperties = field(default_factory=LakehouseSqlEndpointProperties)


@dataclass
class LakehouseAPIData(BaseItemAPIData):
    folderId: str = None
    properties: LakehouseProperties = field(default_factory=LakehouseProperties)


class Lakehouse(BaseWorkspaceItem[LakehouseAPIData]):
    """
    REF: https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items
    """
    def __init__(
        self,
        workspace_id: str,
        name: str,
        description: str = None,
        folder: WorkspaceFolder = None,
        enable_schemas: bool = True,
        api_data: LakehouseAPIData = None
    ):
        depends_on = None
        folder = None if not isinstance(folder, WorkspaceFolder) else folder
        if folder is not None:
            depends_on = [
                WorkspaceItemDependency(folder, WorkspaceItemDependencyType.FOLDER, "folderId")
            ]
        description = description or "New Lakehouse"
        item = FabricItem[LakehouseAPIData](
            displayName=name,
            description=description,
            folderId=folder,
            creationPayload={
                "enableSchemas": enable_schemas
            },
            api_data=api_data
        )
        super().__init__(
            create_type_fn=Lakehouse.from_json,
            base_item_url=ITEM_PATH,
            workspace_id=workspace_id,
            item=item,
            depends_on=depends_on
        )

    @staticmethod
    def from_json(item: dict) -> "Lakehouse":
        kwargs = item.copy()
        if "properties" not in item.keys():
            item["properties"] = {
                "sqlEndpointProperties": {}
            }
        if "sqlEndpointProperties" not in item["properties"]:
            item["properties"]["sqlEndpointProperties"] = {}
        kwargs["properties"] = LakehouseProperties(**item["properties"])
        kwargs["properties"].sqlEndpointProperties = LakehouseSqlEndpointProperties(
            **item["properties"]["sqlEndpointProperties"]
        )
        api_data = LakehouseAPIData(**kwargs)
        return Lakehouse(
            workspace_id=api_data.workspaceId,
            name=api_data.displayName,
            description=api_data.description,
            api_data=api_data
        )

    @staticmethod
    def get_by_name(workspace_id: str, name: str) -> "Lakehouse":
        return BaseWorkspaceItem.get_by_name(
            create_item_type_fn=Lakehouse.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            name=name
        )

    @staticmethod
    def get_by_id(workspace_id: str, id: str) -> "Lakehouse":
        return BaseWorkspaceItem.get_by_id(
            create_item_type_fn=Lakehouse.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            id=id
        )

    @staticmethod
    def list(workspace_id: str) -> list["Lakehouse"]:
        return [
            Lakehouse.from_json(item)
            for item in BaseWorkspaceItem.list(
                workspace_id=workspace_id,
                base_item_url=ITEM_PATH
            )
        ]

    def create(
            self,
            timeout: int = 90,
            max_retry_seconds_at_202: int = 5,
            wait_for_completion: bool = True
    ) -> None:
        super().create(
            wait_for_completion=wait_for_completion,
            timeout=timeout,
            max_retry_seconds_at_202=max_retry_seconds_at_202
        )
        if not wait_for_completion:
            return

        logger.info(f"Lakehouse '{self.item.api.displayName}' created. Waiting for lakehouse SQL endpoint provisioning to complete...")

        retry_after = 5
        retry_sum = 0
        while True:
            state = self.fetch().item.api.properties.sqlEndpointProperties.provisioningStatus
            logger.info(f"Lakehouse '{self.item.api.displayName}' current state: {state}")
            if state != "InProgress":
                logger.info(f"Lakehouse '{self.item.api.displayName}' SQL endpoint provisioning completed with state: {state}.")
                break

            retry_sum += retry_after
            if retry_sum > timeout:
                logger.warning(f"Timeout after {timeout}s")
                raise TimeoutError(f"Timeout while waiting for lakehouse SQL endpoint provisioning. Payload: {self.item.fields}")
            time.sleep(retry_after)
