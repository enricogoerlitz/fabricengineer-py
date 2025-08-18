import os
import requests

from fabricengineer.api.fabric.client.workspace import FabricAPIWorkspaceClient
from fabricengineer.api.auth import MicrosoftExtraSVC


class FabricAPIClient:
    def __init__(
        self,
        msf_svc: MicrosoftExtraSVC = None,
        api_version: str = "v1"
    ):
        self._msf_svc = msf_svc
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
        if path is None:
            return ""
        prep_path = path if path.startswith("/") else f"/{path}"
        return prep_path

    def _get_token(self) -> str:
        if self._msf_svc is None and "notebookutils" not in globals():
            return "{TOKEN_NOT_SET}"
        elif "notebookutils" in globals():
            token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")  # noqa: F821 # type: ignore
            return token
        token = self._msf_svc.token()
        return token


def get_svc() -> MicrosoftExtraSVC | None:
    if "notebookutils" in globals():
        return None

    tenant_id = os.environ.get("MICROSOFT_TENANT_ID")
    client_id = os.environ.get("SVC_MICROSOFT_FABRIC_CLIENT_ID")
    client_secret = os.environ.get("SVC_MICROSOFT_FABRIC_SECRET_VALUE")

    if not all([tenant_id, client_id, client_secret]):
        return None

    return MicrosoftExtraSVC(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )


global fabric_client
fabric_client = FabricAPIClient(msf_svc=get_svc(), api_version="v1")


def set_global_fabric_client(
        msf_svc: MicrosoftExtraSVC = None,
        api_version: str = "v1"
) -> FabricAPIClient:
    global fabric_client
    fabric_client = FabricAPIClient(msf_svc=msf_svc, api_version=api_version)
    return fabric_client
