import requests


class FabricAPIWorkspaceClient:
    def __init__(self, client):
        self._client = client
        self._base_url = f"{client.base_url}/workspaces"

    def get(self, workspace_id: str = None, item_path: str = None) -> requests.Response:
        url = self._url(workspace_id, item_path)
        resp: requests.Response = requests.get(url, headers=self._client.headers)
        return resp

    def post(self, workspace_id: str = None, item_path: str = None, payload: dict = None) -> requests.Response:
        url = self._url(workspace_id, item_path)
        resp: requests.Response = requests.post(
            url,
            headers=self._client.headers,
            json=payload
        )
        return resp

    def patch(self, workspace_id: str = None, item_path: str = None, payload: dict = None) -> requests.Response:
        url = self._url(workspace_id, item_path)
        resp: requests.Response = requests.patch(
            url,
            headers=self._client.headers,
            json=payload
        )
        return resp

    def put(self, workspace_id: str = None, item_path: str = None, payload: dict = None) -> requests.Response:
        url = self._url(workspace_id, item_path)
        resp: requests.Response = requests.put(
            url,
            headers=self._client.headers,
            json=payload
        )
        return resp

    def delete(self, workspace_id: str = None, item_path: str = None) -> requests.Response:
        url = self._url(workspace_id, item_path)
        resp: requests.Response = requests.delete(url, headers=self._client.headers)
        return resp

    def _url(self, workspace_id: str = None, item_path: str = None) -> str:
        item_path = self._client._prep_path(item_path)
        if workspace_id is None:
            return f"{self._base_url}{item_path}"
        url = f"{self._base_url}/{workspace_id}{item_path}"
        return url
