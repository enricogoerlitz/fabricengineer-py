import requests


class FabricAPIWorkspaceClient:
    """Client to interact with Fabric Workspaces API endpoints."""
    def __init__(self, client):
        self._client = client
        self._base_url = f"{client.base_url}/workspaces"

    def get(
            self,
            workspace_id: str = None,
            item_path: str = None
    ) -> requests.Response:
        """
        Get a workspace or a specific item within a workspace.

        Args:
            workspace_id (str, optional): The ID of the workspace to retrieve.
            item_path (str, optional): The path to a specific item within the workspace.

        Returns:
            requests.Response: The response from the API.
        """
        self._client.check_headers_auth()
        url = self._url(workspace_id, item_path)
        resp = requests.get(url, headers=self._client.headers)
        return resp

    def post(
            self,
            workspace_id: str = None,
            item_path: str = None,
            payload: dict = None
    ) -> requests.Response:
        """
        Create a new workspace or item within a workspace.

        Args:
            workspace_id (str, optional): The ID of the workspace to create the item in.
            item_path (str, optional): The path to the specific item within the workspace.
            payload (dict, optional): The data to include in the request body.

        Returns:
            requests.Response: The response from the API.
        """
        self._client.check_headers_auth()
        url = self._url(workspace_id, item_path)
        resp = requests.post(
            url,
            headers=self._client.headers,
            json=payload
        )
        return resp

    def patch(
            self,
            workspace_id: str = None,
            item_path: str = None,
            payload: dict = None
    ) -> requests.Response:
        """
        Update an existing workspace or item within a workspace.

        Args:
            workspace_id (str, optional): The ID of the workspace to update.
            item_path (str, optional): The path to the specific item within the workspace.
            payload (dict, optional): The data to include in the request body.

        Returns:
            requests.Response: The response from the API.
        """
        self._client.check_headers_auth()
        url = self._url(workspace_id, item_path)
        resp = requests.patch(
            url,
            headers=self._client.headers,
            json=payload
        )
        return resp

    def put(
        self,
        workspace_id: str = None,
        item_path: str = None,
        payload: dict = None
    ) -> requests.Response:
        """
        Update an existing workspace or item within a workspace.

        Args:
            workspace_id (str, optional): The ID of the workspace to update.
            item_path (str, optional): The path to the specific item within the workspace.
            payload (dict, optional): The data to include in the request body.

        Returns:
            requests.Response: The response from the API.
        """
        self._client.check_headers_auth()
        url = self._url(workspace_id, item_path)
        resp = requests.put(
            url,
            headers=self._client.headers,
            json=payload
        )
        return resp

    def delete(
        self,
        workspace_id: str = None,
        item_path: str = None
    ) -> requests.Response:
        """
        Delete an existing workspace or item within a workspace.

        Args:
            workspace_id (str, optional): The ID of the workspace to delete.
            item_path (str, optional): The path to the specific item within the workspace.

        Returns:
            requests.Response: The response from the API.
        """
        self._client.check_headers_auth()
        url = self._url(workspace_id, item_path)
        resp = requests.delete(url, headers=self._client.headers)
        return resp

    def _url(
        self,
        workspace_id: str = None,
        item_path: str = None
    ) -> str:
        """Construct the full URL for the API request."""
        item_path = self._client._prep_path(item_path)
        if workspace_id is None:
            return f"{self._base_url}{item_path}"
        url = f"{self._base_url}/{workspace_id}{item_path}"
        return url
