import requests

from fabricengineer.api.fabric.client.workspace import FabricAPIWorkspaceClient


class FabricAPIClient:
    def __init__(
        self,
        auth_type: str = None,
        api_version: str = "v1"
    ):
        self._auth_type = auth_type
        self._base_url = f"https://api.fabric.microsoft.com/{api_version}"
        self.refresh_headers()
        self._workspaces = FabricAPIWorkspaceClient(self)

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def workspaces(self) -> FabricAPIWorkspaceClient:
        return self._workspaces

    @property
    def headers(self) -> dict:
        return self._headers

    def refresh_headers(self) -> None:
        self._headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json"
        }

    def get(self, path: str) -> requests.Response:
        url = self._url(path)
        resp: requests.Response = requests.get(url, headers=self.headers)
        return resp

    def post(self, path: str, payload: dict) -> requests.Response:
        url = self._url(path)
        resp: requests.Response = requests.post(
            url,
            headers=self.headers,
            json=payload
        )
        return resp

    def patch(self, path: str, payload: dict) -> requests.Response:
        url = self._url(path)
        resp: requests.Response = requests.patch(
            url,
            headers=self.headers,
            json=payload
        )
        return resp

    def put(self, path: str, payload: dict) -> requests.Response:
        url = self._url(path)
        resp: requests.Response = requests.put(
            url,
            headers=self.headers,
            json=payload
        )
        return resp

    def delete(self, path: str) -> requests.Response:
        url = self._url(path)
        resp: requests.Response = requests.delete(url, headers=self.headers)
        return resp

    def _url(self, path: str) -> str:
        path = self._prep_path(path)
        url = f"{self._base_url}{path}"
        return url

    def _prep_path(self, path: str) -> str:
        prep_path = path if path.startswith("/") else f"/{path}"
        return prep_path

    def _get_token(self) -> str:
        token = "{TOKEN_NOT_SET}"
        if self._auth_type is None or self._auth_type == "default":
            token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")  # noqa: F821 # type: ignore
        return token


global fabric_client
fabric_client = None


def create_global_fabric_client(auth_type: str = None, api_version: str = "v1") -> FabricAPIClient:
    global fabric_client
    fabric_client = FabricAPIClient(auth_type=auth_type, api_version=api_version)
    return fabric_client
