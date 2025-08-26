import enum
import requests

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Generic, List, TypeVar, Any, Iterator, Callable

from fabricengineer.api.fabric.client.fabric import fabric_client
from fabricengineer.api.utils import check_http_response, http_wait_for_completion_after_202
from fabricengineer.logging import logger


TItemAPIData = TypeVar("TItemAPIData")


@dataclass
class BaseItemAPIData:
    id: str
    workspaceId: str
    displayName: str
    description: str
    type: str


@dataclass
class FabricItem(Generic[TItemAPIData]):
    fields: dict[str, Any]
    api: Optional[TItemAPIData] = None

    def __init__(self, api_data: Optional[TItemAPIData] = None, **fields: dict):
        self.fields = fields
        self.api = api_data


class ItemDefinitionInterface(ABC):
    @abstractmethod
    def get_definition(self) -> dict: pass


class CopyItemDefinition(ItemDefinitionInterface):
    def __init__(self, workspace_id: str, id: str, item_uri_name: str):
        self._wid = workspace_id
        self._id = id
        self._item_uri_name = item_uri_name

    def get_definition(self) -> dict:
        """Get the item definition by fetching it from the API."""
        url = f"/workspaces/{self._wid}/{self._item_uri_name}/{self._id}/getDefinition"
        try:
            resp = fabric_client().post(url, payload={})
            check_http_response(resp)
            definition = resp.json()
            if resp.status_code == 202:
                definition = http_wait_for_completion_after_202(resp, retry_max_seconds=1)
            return definition["definition"]
        except Exception as e:
            logger.error(f"Error fetching definition at url '{url}'.\n{e}")
            raise e


class WorkspaceItemDependencyType(enum.Enum):
    FOLDER = "Folder"
    WAREHOUSE = "Warehouse"
    LAKEHOUSE = "Lakehouse"
    NOTEBOOK = "Notebook"
    DATA_PIPELINE = "DataPipeline"
    VARIABLE_LIBRARY_VALUE = "VariableLibraryValue"


@dataclass
class WorkspaceItemDependency:
    item: "BaseWorkspaceItem"
    type: WorkspaceItemDependencyType
    field: str


class BaseWorkspaceItem(Generic[TItemAPIData]):
    """Base class for all workspace items."""

    def __init__(
            self,
            create_type_fn: Callable,
            base_item_url: str,
            item: FabricItem[TItemAPIData],
            workspace_id: str = None,
            depends_on: list[WorkspaceItemDependency] = None
    ):
        self._create_item_type_fn = create_type_fn
        self._base_item_url = base_item_url
        self._workspace_id = workspace_id
        self._item: FabricItem[TItemAPIData] = item
        self._upstream_items = depends_on or []
        self._downstream_items = []
        self._register_at_upstream_items(depends_on)

    @property
    def item(self) -> FabricItem[TItemAPIData]:
        return self._item

    @property
    def upstream_items(self) -> list[WorkspaceItemDependency]:
        return self._upstream_items

    @property
    def downstream_items(self) -> list["BaseWorkspaceItem"]:
        return self._downstream_items

    @staticmethod
    def get_by_id(
            create_item_type_fn: Callable,
            workspace_id: str,
            base_item_url: str,
            id: str,
            log_err: bool = True
    ) -> Any:
        """Get a workspace item by its ID."""
        item_path = f"{base_item_url}/{id}"
        try:
            resp = fabric_client().workspaces.get(workspace_id, item_path)
            check_http_response(resp)
            item = resp.json()
            return create_item_type_fn(item)
        except Exception as e:
            if log_err:
                logger.error(f"Error fetching item at path '{item_path}'.\n{e}")
            raise e

    @staticmethod
    def get_by_name(
            create_item_type_fn: Callable,
            workspace_id: str,
            base_item_url: str,
            name: str,
            log_err: bool = True
    ) -> Any:
        """Get a workspace item by its name."""
        item_path = base_item_url
        try:
            resp = fabric_client().workspaces.get(workspace_id, item_path)
            check_http_response(resp)
            for item in resp.json()["value"]:
                if item["displayName"] == name:
                    return create_item_type_fn(item)
            raise requests.HTTPError(f"404 Client Error: Item with displayName '{name}' not found.")
        except Exception as e:
            if log_err:
                logger.error(f"Error fetching item at path '{item_path}'.\n{e}")
            raise e

    @staticmethod
    def list(workspace_id: str, base_item_url: str) -> Iterator[dict]:
        """List all items in a workspace."""
        item_path = base_item_url
        try:
            resp = fabric_client().workspaces.get(workspace_id, item_path)
            check_http_response(resp)
            for item in resp.json()["value"]:
                yield item
        except Exception as e:
            logger.error(f"Error fetching item at path '{item_path}': {e}")
            raise e

    def fetch(self, log_err: bool = True) -> "BaseWorkspaceItem":
        """Fetch the latest item data from the API and update the internal state."""
        if self._item.api is None:
            if self._item.fields.get("displayName") is None:
                raise ValueError("Item displayName is required to fetch item by name.")
            self._item.api = BaseWorkspaceItem.get_by_name(
                create_item_type_fn=self._create_item_type_fn,
                workspace_id=self._workspace_id,
                base_item_url=self._base_item_url,
                name=self._item.fields["displayName"],
                log_err=log_err
            ).item.api
            return self

        self._item.api = BaseWorkspaceItem.get_by_id(
            create_item_type_fn=self._create_item_type_fn,
            workspace_id=self._workspace_id,
            base_item_url=self._base_item_url,
            id=self._item.api.id,
            log_err=log_err
        ).item.api
        return self

    def fetch_definition(self) -> dict:
        """Fetch the item definition from the API."""
        try:
            if self._item.api is None:
                self.fetch()
                if self._item.api is None:
                    raise ValueError("Item API is not available")
            url = f"{self._base_item_url}/{self._item.api.id}/getDefinition"
            resp = fabric_client().workspaces.post(self._workspace_id, url, payload={})
            check_http_response(resp)
            if resp.status_code == 202:
                return http_wait_for_completion_after_202(resp, retry_max_seconds=1)
            return resp.json()
        except Exception as e:
            logger.error(f"Error fetching item definition for item '{self}'.\n{e}")
            raise e

    def exists(self) -> bool:
        """Check if the item exists in the workspace by attempting to fetch it."""
        try:
            return self.fetch(log_err=False)._item.api is not None
        except requests.HTTPError as e:
            if "404 Client Error:" in str(e):
                return False
            logger.error(f"Error checking existence of item '{self}'.\n{e}")
            raise e
        except Exception as e:
            logger.error(f"Error checking existence of item '{self}'.\n{e}")
            raise e

    def create(
            self,
            wait_for_completion: bool = True,
            max_retry_seconds_at_202: int = 5,
            timeout: int = 90
    ) -> None:
        """Create the item in the workspace."""
        item_path = self._base_item_url
        payload = self._get_create_payload()
        try:
            resp = fabric_client().workspaces.post(
                workspace_id=self._workspace_id,
                item_path=item_path,
                payload=payload
            )
            check_http_response(resp)

            item = resp.json()
            if resp.status_code == 202 and item is None:
                if not wait_for_completion:
                    return
                item = http_wait_for_completion_after_202(
                    resp=resp,
                    payload=payload,
                    retry_max_seconds=max_retry_seconds_at_202,
                    timeout=timeout
                )

            self._item.api = self._create_item_type_fn(item).item.api
            self.fetch()
        except Exception as e:
            logger.error(f"Error creating item '{self}'.\n{e}")
            raise e

    def create_if_not_exists(
            self,
            wait_for_completion: bool = True,
            max_retry_seconds_at_202: int = 5,
            timeout: int = 90
    ) -> None:
        """Create the item if it does not already exist in the workspace."""
        if self.exists():
            logger.info(f"Item already exists, skipping creation. {self}")
            return
        self.create(
            wait_for_completion=wait_for_completion,
            max_retry_seconds_at_202=max_retry_seconds_at_202,
            timeout=timeout
        )

    def update(self, **fields) -> None:
        """Update the item in the workspace with the provided fields."""
        try:
            if self._item.api is None:
                self.fetch()

            self._item.fields.update(fields)
            payload = self._item.fields
            item_path = f"{self._base_item_url}/{self._item.api.id}"
            resp = fabric_client().workspaces.patch(
                workspace_id=self._workspace_id,
                item_path=item_path,
                payload=payload
            )
            check_http_response(resp)
            item = resp.json()
            self._item.api = self._create_item_type_fn(item).item.api
            self.fetch()
        except Exception as e:
            logger.error(f"Error updating item '{self}'.\n{e}")
            raise e

    def delete(self) -> None:
        """Delete the item from the workspace."""
        try:
            if self._item.api is None:
                self.fetch()

            item_path = f"{self._base_item_url}/{self._item.api.id}"
            resp = fabric_client().workspaces.delete(
                workspace_id=self._workspace_id,
                item_path=item_path
            )
            check_http_response(resp)
        except Exception as e:
            logger.error(f"Error deleting item '{self}'.\n{e}")
            raise e

    def _get_create_payload(self) -> dict:
        """Get the payload for creating the item, resolving any dependencies."""
        for dependency in self._upstream_items:
            if dependency.item.item.api is None:
                raise ValueError(f"Dependency '{dependency.field}' is not created yet.")
            self._item.fields[dependency.field] = dependency.item.item.api.id
        return self._item.fields

    def _register_downstream_item(self, dependency: "BaseWorkspaceItem") -> None:
        """Register a downstream item that depends on this item."""
        if not isinstance(dependency, BaseWorkspaceItem):
            raise ValueError("Dependency must be an instance of BaseWorkspaceItem.")
        self._downstream_items.append(dependency)

    def _register_at_upstream_items(self, dependencies: List[WorkspaceItemDependency]) -> None:
        """Register this item as a downstream item at its upstream dependencies."""
        if dependencies is None:
            return
        dependency: WorkspaceItemDependency
        for dependency in dependencies:
            dependency.item._register_downstream_item(self)

    def __str__(self) -> str:
        item_str = str(self._item)
        workspace_id_str = f"workspaceId='{self._workspace_id}', " if self._workspace_id else ""
        item_str_total = (
            f"{self.__class__.__name__}("
            f"{workspace_id_str}"
            f"item={item_str}"
            f")"
        )
        return item_str_total

    def __repr__(self) -> str:
        return str(self)
