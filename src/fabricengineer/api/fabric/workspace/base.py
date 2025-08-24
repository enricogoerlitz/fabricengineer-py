import requests

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Generic, TypeVar, Any, Iterator, Callable

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


class BaseWorkspaceItem(Generic[TItemAPIData]):
    def __init__(
            self,
            create_type_fn: Callable,
            base_item_url: str,
            item: FabricItem[TItemAPIData],
            workspace_id: str = None
    ):
        self._create_item_type_fn = create_type_fn
        self._base_item_url = base_item_url
        self._workspace_id = workspace_id
        self._item: FabricItem[TItemAPIData] = item

    @property
    def item(self) -> FabricItem[TItemAPIData]:
        return self._item

    @staticmethod
    def get_by_id(
            create_item_type_fn: Callable,
            workspace_id: str,
            base_item_url: str,
            id: str
    ) -> Any:
        item_path = f"{base_item_url}/{id}"
        try:
            resp = fabric_client().workspaces.get(workspace_id, item_path)
            check_http_response(resp)
            item = resp.json()
            return create_item_type_fn(item)
        except Exception as e:
            logger.error(f"Error fetching item at path '{item_path}'.\n{e}")
            raise e

    @staticmethod
    def get_by_name(
            create_item_type_fn: Callable,
            workspace_id: str,
            base_item_url: str,
            name: str
    ) -> Any:
        item_path = base_item_url
        try:
            resp = fabric_client().workspaces.get(workspace_id, item_path)
            check_http_response(resp)
            print("RESP-LIST:", resp.json()["value"])
            for item in resp.json()["value"]:
                if item["displayName"] == name:
                    return create_item_type_fn(item)
            raise requests.HTTPError(f"404 Client Error: Item with displayName '{name}' not found.")
        except Exception as e:
            logger.error(f"Error fetching item at path '{item_path}'.\n{e}")
            raise e

    @staticmethod
    def list(workspace_id: str, base_item_url: str) -> Iterator[dict]:
        item_path = base_item_url
        try:
            resp = fabric_client().workspaces.get(workspace_id, item_path)
            check_http_response(resp)
            for item in resp.json()["value"]:
                yield item
        except Exception as e:
            logger.error(f"Error fetching item at path '{item_path}': {e}")
            raise e

    def fetch(self) -> "BaseWorkspaceItem":
        if self._item.api is None:
            if self._item.fields.get("displayName") is None:
                raise ValueError("Item displayName is required to fetch item by name.")
            self._item.api = BaseWorkspaceItem.get_by_name(
                create_item_type_fn=self._create_item_type_fn,
                workspace_id=self._workspace_id,
                base_item_url=self._base_item_url,
                name=self._item.fields["displayName"]
            ).item.api
            return self

        self._item.api = BaseWorkspaceItem.get_by_id(
            create_item_type_fn=self._create_item_type_fn,
            workspace_id=self._workspace_id,
            base_item_url=self._base_item_url,
            id=self._item.api.id
        ).item.api
        return self

    def fetch_definition(self) -> dict:
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
        try:
            return self.fetch()._item.api is not None
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
        item_path = self._base_item_url
        payload = self._item.fields
        try:
            resp = fabric_client().workspaces.post(
                workspace_id=self._workspace_id,
                item_path=item_path,
                payload=payload
            )
            check_http_response(resp)

            item = resp.json()
            logger.info(f"RESP: {resp.status_code}, {item}")
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
        if self.exists():
            logger.info(f"Item already exists, skipping creation. {self}")
            return
        self.create(
            wait_for_completion=wait_for_completion,
            max_retry_seconds_at_202=max_retry_seconds_at_202,
            timeout=timeout
        )

    def update(self, **fields) -> None:
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

    def _retry_after(self, resp: requests.Response) -> int:
        return min(int(resp.headers.get("Retry-After", 5)), 5)

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
