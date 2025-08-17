import requests
import time

from dataclasses import dataclass
from typing import Optional, Generic, TypeVar, Any, Self, Iterator, Callable

from fabricengineer.api.fabric.client.fabric import fabric_client
from fabricengineer.logging.logger import logger


TItemAPIData = TypeVar("TItemAPIData")


@dataclass
class FabricItem(Generic[TItemAPIData]):
    fields: dict[str, Any]
    apiData: Optional[TItemAPIData] = None

    def __init__(self, apiData: Optional[TItemAPIData] = None, **fields: dict):
        self.fields = fields
        self.apiData = apiData


class BaseWorkspaceItem(Generic[TItemAPIData]):
    def __init__(
            self,
            create_type_fn: Callable,
            base_item_url: str,
            workspace_id: str,
            item: FabricItem[TItemAPIData]
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
            create_type_fn: Callable,
            workspace_id: str,
            base_item_url: str,
            id: str
    ) -> Any:
        item_path = f"{base_item_url}/{id}"
        resp = fabric_client.workspaces.get(workspace_id, item_path)
        resp.raise_for_status()
        item = resp.json()
        return create_type_fn(item)

    @staticmethod
    def get_by_name(
            create_type_fn: Callable,
            workspace_id: str,
            base_item_url: str,
            name: str
    ) -> Any:
        item_path = base_item_url
        resp = fabric_client.workspaces.get(workspace_id, item_path)
        resp.raise_for_status()
        for item in resp.json()["value"]:
            if item["displayName"] == name:
                return create_type_fn(item)
        raise requests.HTTPError(f"404 Client Error: Item with displayName '{name}' not found")

    @staticmethod
    def list(workspace_id: str, base_item_url: str) -> Iterator[dict]:
        item_path = base_item_url
        resp = fabric_client.workspaces.get(workspace_id, item_path)
        resp.raise_for_status()
        for item in resp.json()["value"]:
            yield item

    def fetch(self) -> Self:
        if self._item.apiData is None:
            self._item.apiData = BaseWorkspaceItem.get_by_name(
                create_type_fn=self._create_item_type_fn,
                workspace_id=self._workspace_id,
                base_item_url=self._base_item_url,
                name=self._item.fields["displayName"]
            ).item.apiData
            return self

        self._item.apiData = BaseWorkspaceItem.get_by_id(
            create_type_fn=self._create_item_type_fn,
            workspace_id=self._workspace_id,
            base_item_url=self._base_item_url,
            id=self._item.apiData.id
        ).item.apiData
        return self

    def exists(self) -> bool:
        try:
            return self.fetch()._item.apiData is not None
        except requests.HTTPError as e:
            if "404 Client Error:" in str(e):
                return False
            raise e
        except Exception as e:
            raise e

    def create(self) -> None:
        item_path = self._base_item_url
        payload = self._item.fields
        resp = fabric_client.workspaces.post(
            workspace_id=self._workspace_id,
            item_path=item_path,
            payload=payload
        )
        resp.raise_for_status()

        item = resp.json()
        if resp.status_code == 202 and item is None:
            item = self._wait_after_202(resp, payload)

        self._item.apiData = self._create_item_type_fn(item).item.apiData
        self.fetch()

    def create_if_not_exists(self) -> None:
        if self.exists():
            return
        self.create()

    def update(self, **fields) -> None:
        if self._item.apiData is None:
            self.fetch()

        self._item.fields.update(fields)
        payload = self._item.fields
        item_path = f"{self._base_item_url}/{self._item.apiData.id}"
        resp = fabric_client.workspaces.patch(
            workspace_id=self._workspace_id,
            item_path=item_path,
            payload=payload
        )
        resp.raise_for_status()
        item = resp.json()
        self._item.apiData = self._create_item_type_fn(item).item.apiData
        self.fetch()

    def delete(self) -> None:
        if self._item.apiData is None:
            self.fetch()

        item_path = f"{self._base_item_url}/{self._item.apiData.id}"
        resp = fabric_client.workspaces.delete(
            workspace_id=self._workspace_id,
            item_path=item_path
        )
        resp.raise_for_status()

    def _wait_after_202(self, resp: requests.Response, payload: dict, timeout: int = 90) -> requests.Response:
        op_id = resp.headers["x-ms-operation-id"]
        op_location = resp.headers["Location"]
        retry = self._retry_after(resp)
        logger.info(f"Status=202, Operation ID: {op_id}, Location: {op_location}, Retry after: {retry}s")

        retry_sum = 0
        warehouse = None
        while True:
            time.sleep(retry)
            resp_retry = requests.get(op_location, headers=fabric_client.headers)

            if resp_retry.json()["status"] == "Succeeded":
                res = requests.get(resp_retry.headers["Location"], headers=fabric_client.headers)
                res.raise_for_status()
                warehouse = res.json()
                break

            retry = self._retry_after(resp_retry)
            retry_sum += retry
            if retry_sum > timeout:
                logger.warning(f"Timeout after {timeout}s")
                raise TimeoutError(f"Timeout while waiting for item creation. Payload: {payload}")

            logger.info(f"Wait for more {retry}s")

        return warehouse

    def _retry_after(self, resp: requests.Response) -> int:
        return min(int(resp.headers.get("Retry-After", 5)), 5)

    def __str__(self) -> str:
        item_str = str(self._item)
        item_str_total = (
            f"{self.__class__.__name__}("
            f"workspaceId='{self._workspace_id}', "
            f"item={item_str}"
        )
        return item_str_total

    def __repr__(self) -> str:
        return str(self)
