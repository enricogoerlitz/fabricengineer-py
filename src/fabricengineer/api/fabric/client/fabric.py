import os
import requests

from fabricengineer.setup import notebookutils
from fabricengineer.api.fabric.client.utils import retry
from fabricengineer.api.fabric.client.workspace import FabricAPIWorkspaceClient
from fabricengineer.api.auth import MicrosoftExtraSVC
from fabricengineer.logging import logger


def get_env_svc() -> MicrosoftExtraSVC | None:
    """
    Get the MicrosoftExtraSVC instance from environment variables.
    Uses the following environment variables to create service principal:
    - MICROSOFT_TENANT_ID
    - SVC_MICROSOFT_FABRIC_CLIENT_ID
    - SVC_MICROSOFT_FABRIC_SECRET_VALUE

    If running in a notebook environment, return None.

    Returns:
        MicrosoftExtraSVC | None: The MicrosoftExtraSVC instance or None if not found.
    """
    if notebookutils() is not None:
        logger.info("Using notebookutils for authentication.")
        return None

    tenant_id = os.environ.get("MICROSOFT_TENANT_ID")
    client_id = os.environ.get("SVC_MICROSOFT_FABRIC_CLIENT_ID")
    client_secret = os.environ.get("SVC_MICROSOFT_FABRIC_SECRET_VALUE")

    if not all([tenant_id, client_id, client_secret]):
        expected_env_vars = ["MICROSOFT_TENANT_ID", "SVC_MICROSOFT_FABRIC_CLIENT_ID", "SVC_MICROSOFT_FABRIC_SECRET_VALUE"]
        msg = (
            f"Microsoft Fabric service principal environment variables not fully set. "
            f"Missing environment variables: {', '.join(expected_env_vars)}"
        )
        logger.warning(msg)
        return None

    logger.info("Using service principal for authentication.")
    return MicrosoftExtraSVC(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )


class FabricAPIClient:
    """Client for interacting with the Microsoft Fabric API."""
    def __init__(
        self,
        msf_svc: MicrosoftExtraSVC = None,
        api_version: str = "v1"
    ):
        self._msf_svc = msf_svc or get_env_svc()
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
        """ Refresh the authorization headers with a new token."""
        self._headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json"
        }

    @retry(count=3, on=[429], delay=15)
    def get(self, path: str) -> requests.Response:
        """
        Send a GET request to the specified API endpoint.

        Args:
            path (str): The API endpoint path.

        Returns:
            requests.Response: The response from the API.
        """
        self.check_headers_auth()
        url = self._url(path)
        resp = requests.get(url, headers=self.headers)
        return resp

    @retry(count=3, on=[429], delay=15)
    def post(self, path: str, payload: dict) -> requests.Response:
        """
        Send a POST request to the specified API endpoint.

        Args:
            path (str): The API endpoint path.
            payload (dict): The JSON payload to include in the request.

        Returns:
            requests.Response: The response from the API.
        """
        self.check_headers_auth()
        url = self._url(path)
        resp = requests.post(
            url,
            headers=self.headers,
            json=payload
        )
        return resp

    @retry(count=3, on=[429], delay=15)
    def patch(self, path: str, payload: dict) -> requests.Response:
        """
        Send a PATCH request to the specified API endpoint.

        Args:
            path (str): The API endpoint path.
            payload (dict): The JSON payload to include in the request.

        Returns:
            requests.Response: The response from the API.
        """
        self.check_headers_auth()
        url = self._url(path)
        resp = requests.patch(
            url,
            headers=self.headers,
            json=payload
        )
        return resp

    @retry(count=3, on=[429], delay=15)
    def put(self, path: str, payload: dict) -> requests.Response:
        """
        Send a PUT request to the specified API endpoint.

        Args:
            path (str): The API endpoint path.
            payload (dict): The JSON payload to include in the request.

        Returns:
            requests.Response: The response from the API.
        """
        self.check_headers_auth()
        url = self._url(path)
        resp = requests.put(
            url,
            headers=self.headers,
            json=payload
        )
        return resp

    @retry(count=3, on=[429], delay=15)
    def delete(self, path: str) -> requests.Response:
        """
        Send a DELETE request to the specified API endpoint.

        Args:
            path (str): The API endpoint path.

        Returns:
            requests.Response: The response from the API.
        """
        self.check_headers_auth()
        url = self._url(path)
        resp = requests.delete(url, headers=self.headers)
        return resp

    def check_headers_auth(self) -> None:
        """
        Check if the authorization headers are present and valid.
        """
        token = self.headers.get("Authorization", "").replace("Bearer ", "")
        if len(token) < 10:
            raise PermissionError("Authorization header is missing.")

    def _url(self, path: str) -> str:
        """ Construct the full URL for the API request."""
        path = self._prep_path(path)
        url = f"{self._base_url}{path}"
        return url

    def _prep_path(self, path: str) -> str:
        """Prepare the API endpoint path."""
        if path is None or path == "":
            return ""
        prep_path = path if path.startswith("/") else f"/{path}"
        return prep_path

    def _get_token(self) -> str:
        """
        Retrieve an authentication token using the available method.

        If notebookutils is available, use it to get the token.
        If notebookutils is not available, fall back to Microsoft Fabric Service Principal.

        Returns:
            str: The authentication token.
        """
        if self._msf_svc is None and notebookutils() is None:
            logger.warning("No authentication method available. Token is empty.")
            return ""
        elif notebookutils() is not None:
            logger.info("Getting token via notebookutils.")
            token = notebookutils().credentials.getToken("https://api.fabric.microsoft.com")  # noqa: F821 # type: ignore
            return token
        logger.info("Getting token via Microsoft Fabric Service Principal.")
        token = self._msf_svc.token()
        return token


global fabric_client_instance
fabric_client_instance = FabricAPIClient(msf_svc=get_env_svc(), api_version="v1")


def fabric_client() -> FabricAPIClient:
    """ Get the global FabricAPIClient instance."""
    return fabric_client_instance


def set_global_fabric_client(
        msf_svc: MicrosoftExtraSVC = None,
        api_version: str = "v1"
) -> FabricAPIClient:
    """Set the global FabricAPIClient instance."""
    global fabric_client_instance
    fabric_client_instance = FabricAPIClient(msf_svc=msf_svc, api_version=api_version)
    return fabric_client_instance
