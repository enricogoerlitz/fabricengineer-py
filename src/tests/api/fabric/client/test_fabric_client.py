"""

uv run pytest src/tests/api/fabric/client -v
"""
import uuid

from fabricengineer.api.fabric.client.fabric import FabricAPIClient, get_env_svc, set_global_fabric_client
from fabricengineer.api.auth import MicrosoftExtraSVC


class TestUtilsFunctions:

    def test_get_env_svc(self):
        msf_svc = get_env_svc()

        assert msf_svc is not None
        assert isinstance(msf_svc, MicrosoftExtraSVC)

        assert len(msf_svc.tenant_id) == 36
        assert len(msf_svc.client_id) == 36
        assert len(msf_svc.client_secret) == 40


class TestFabricAPIClient:
    """Test suite for FabricAPIClient."""
    def test_initialize_fabric_client(self, msf_svc: MicrosoftExtraSVC):
        client = FabricAPIClient(msf_svc=msf_svc, api_version="v1")

        assert client.base_url == "https://api.fabric.microsoft.com/v1"
        assert client.headers is not None
        assert "Authorization" in client.headers
        assert "Content-Type" in client.headers
        assert len(client.headers.get("Authorization", "")) > len("Bearer TOKEN")
        assert client.headers["Content-Type"] == "application/json"
        assert client.workspaces is not None

    def test_initialize_fabric_client_svc_from_env(self):
        client = FabricAPIClient(api_version="v1")

        assert client.base_url == "https://api.fabric.microsoft.com/v1"
        assert client.headers is not None
        assert "Authorization" in client.headers
        assert "Content-Type" in client.headers
        assert len(client.headers.get("Authorization", "")) > len("Bearer TOKEN")
        assert client.headers["Content-Type"] == "application/json"
        assert client.workspaces is not None

    def test_global_fabric_import(self):
        from fabricengineer.api.fabric.client.fabric import fabric_client

        assert fabric_client().base_url == "https://api.fabric.microsoft.com/v1"
        assert fabric_client().headers is not None
        assert "Authorization" in fabric_client().headers
        assert "Content-Type" in fabric_client().headers
        assert len(fabric_client().headers.get("Authorization", "")) > len("Bearer TOKEN")
        assert fabric_client().headers["Content-Type"] == "application/json"
        assert fabric_client().workspaces is not None

    def test_set_global_fabric_client(self, msf_svc: MicrosoftExtraSVC):
        from fabricengineer.api.fabric.client.fabric import fabric_client

        assert fabric_client().headers is not None
        client_hash_before = fabric_client().__hash__()

        set_global_fabric_client(msf_svc=msf_svc)
        # from fabricengineer.api.fabric.client.fabric import fabric_client

        client_hash_after = fabric_client().__hash__()
        assert client_hash_before != client_hash_after

    def test_refresh_headers(self):
        client = FabricAPIClient(api_version="v1")
        headers_before = client.headers.copy()

        assert headers_before is not None
        assert len(headers_before.get("Authorization", "")) > len("Bearer TOKEN")
        assert headers_before.get("Content-Type", "") == "application/json"

        client.refresh_headers()
        headers_after = client.headers.copy()

        assert headers_after != headers_before
        assert headers_after is not None
        assert len(headers_after.get("Authorization", "")) > len("Bearer TOKEN")
        assert headers_after.get("Content-Type", "") == "application/json"

    def test_prep_path(self):
        """Test the _prep_path method with various path inputs."""
        client = FabricAPIClient(api_version="v1")

        # Test path that already starts with /
        assert client._prep_path("/workspaces") == "/workspaces"
        assert client._prep_path("/workspaces/123/items") == "/workspaces/123/items"

        # Test path that doesn't start with /
        assert client._prep_path("workspaces") == "/workspaces"
        assert client._prep_path("workspaces/123/items") == "/workspaces/123/items"

        # Test empty path
        assert client._prep_path("") == ""

        # Test None path
        assert client._prep_path(None) == ""

    def test_url(self):
        """Test the _url method for correct URL construction."""
        client = FabricAPIClient(api_version="v1")

        # Test with path that starts with /
        assert client._url("/workspaces") == "https://api.fabric.microsoft.com/v1/workspaces"
        assert client._url("/workspaces/123/items") == "https://api.fabric.microsoft.com/v1/workspaces/123/items"

        # Test with path that doesn't start with /
        assert client._url("workspaces") == "https://api.fabric.microsoft.com/v1/workspaces"
        assert client._url("workspaces/123/items") == "https://api.fabric.microsoft.com/v1/workspaces/123/items"

        # Test with empty path
        assert client._url("") == "https://api.fabric.microsoft.com/v1"

        # Test with None path
        assert client._url(None) == "https://api.fabric.microsoft.com/v1"

    def test_get_token_with_msf_svc(self, msf_svc: MicrosoftExtraSVC):
        """Test the _get_token method when using MicrosoftExtraSVC."""
        client = FabricAPIClient(msf_svc=msf_svc, api_version="v1")

        token = client._get_token()

        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 0
        # Should be a JWT token (basic format check)
        assert "." in token  # JWT tokens have dots separating parts

    def test_get_token_without_msf_svc_and_no_notebookutils(self):
        """Test the _get_token method when no auth service is available."""
        client = FabricAPIClient(api_version="v1")
        client._msf_svc = None

        # Since we're not in a Fabric environment (no notebookutils) and no msf_svc
        token = client._get_token()

        assert client._msf_svc is None
        assert token == ""

    def test_get(self, fabric_client: FabricAPIClient, workspace_id):
        """Test the get method."""
        response = fabric_client.get(f"/workspaces/{workspace_id}")

        assert response is not None
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_get_list(self, fabric_client: FabricAPIClient):
        """Test the get method."""
        response = fabric_client.get("/workspaces")

        assert response is not None
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "value" in data
        assert isinstance(data["value"], list)

    def test_post_folders(self, fabric_client: FabricAPIClient, workspace_id):
        """Test the post method."""
        name = f"F_{uuid.uuid4().hex[:8]}"
        url = f"/workspaces/{workspace_id}/folders"
        payload = {
            "displayName": name
        }
        response = fabric_client.post(url, payload=payload)

        assert response is not None
        assert response.status_code == 201
        data = response.json()
        assert isinstance(data, dict)

        # Delete folder
        folder_id = data.get("id")
        delete_url = f"{url}/{folder_id}"
        delete_response = fabric_client.delete(delete_url)

        assert delete_response is not None
        assert delete_response.status_code == 200

    def test_patch_folder(self, fabric_client: FabricAPIClient, workspace_id):
        """Test the patch method by updating a folder."""
        # First create a folder
        name = f"F_{uuid.uuid4().hex[:8]}"
        url = f"/workspaces/{workspace_id}/folders"
        payload = {
            "displayName": name
        }
        response = fabric_client.post(url, payload=payload)

        assert response.status_code == 201
        data = response.json()
        folder_id = data.get("id")

        # Now patch/update the folder
        new_name = f"F_UPDATED_{uuid.uuid4().hex[:8]}"
        patch_url = f"{url}/{folder_id}"
        patch_payload = {
            "displayName": new_name
        }
        patch_response = fabric_client.patch(patch_url, payload=patch_payload)

        assert patch_response is not None
        assert patch_response.status_code == 200
        patch_data = patch_response.json()
        assert isinstance(patch_data, dict)
        assert patch_data.get("displayName") == new_name

        # Clean up - delete the folder
        delete_response = fabric_client.delete(patch_url)
        assert delete_response.status_code == 200

    def test_delete_folder(self, fabric_client: FabricAPIClient, workspace_id):
        """Test the delete method standalone."""
        # First create a folder to delete
        name = f"F_{uuid.uuid4().hex[:8]}"
        url = f"/workspaces/{workspace_id}/folders"
        payload = {
            "displayName": name
        }
        response = fabric_client.post(url, payload=payload)

        assert response.status_code == 201
        data = response.json()
        folder_id = data.get("id")

        # Now delete the folder
        delete_url = f"{url}/{folder_id}"
        delete_response = fabric_client.delete(delete_url)

        assert delete_response is not None
        assert delete_response.status_code == 200

        # Verify the folder is actually deleted by trying to get it
        get_response = fabric_client.get(delete_url)
        assert get_response.status_code == 404  # Should not be found
