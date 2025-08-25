"""
End-to-end and unit-style tests for FabricAPIClient utilities and HTTP operations.

How to run locally:
- Ensure authentication is configured (fixtures like `msf_svc`, `fabric_client`, and `workspace_id`
  must provide valid credentials and a reachable Fabric environment).
- Then run:
    uv run pytest src/tests/api/fabric/client -v

Notes:
- These tests assert header composition, URL construction, token retrieval behavior,
  and CRUD operations against Fabric workspaces (folders lifecycle).
- Live tests require a valid Fabric workspace and permissions.
"""

import uuid
import pytest

from fabricengineer.api.fabric.client.fabric import FabricAPIClient, get_env_svc, set_global_fabric_client
from fabricengineer.api.auth import MicrosoftExtraSVC

# Centralize API version and base URL used in assertions to keep tests DRY.
API_VERSION = "v1"
BASE_URL = f"https://api.fabric.microsoft.com/{API_VERSION}"


class TestUtilsFunctions:
    """Tests for utility helpers that prepare services for the client."""

    def test_get_env_svc(self):
        """Build MicrosoftExtraSVC from env and sanity-check required fields."""
        # Act
        msf_svc = get_env_svc()

        # Assert
        assert msf_svc is not None
        assert isinstance(msf_svc, MicrosoftExtraSVC)

        # Light sanity checks on expected lengths for UUID/secret-like values
        assert len(msf_svc.tenant_id) == 36
        assert len(msf_svc.client_id) == 36
        assert len(msf_svc.client_secret) == 40


class TestFabricAPIClient:
    """Initialization, header/auth checks, URL building, token retrieval, and CRUD happy-paths."""

    def test_initialize_fabric_client(self, msf_svc: MicrosoftExtraSVC):
        """Initialize with explicit MicrosoftExtraSVC and verify base fields and sub-client availability."""
        # Arrange & Act
        client = FabricAPIClient(msf_svc=msf_svc, api_version=API_VERSION)

        # Assert
        assert client.base_url == BASE_URL
        assert client.headers is not None
        assert "Authorization" in client.headers
        assert "Content-Type" in client.headers
        # Token should be non-empty beyond the Bearer prefix
        assert len(client.headers.get("Authorization", "")) > len("Bearer TOKEN")
        assert client.headers["Content-Type"] == "application/json"
        # Sub-client initialization
        assert client.workspaces is not None

    def test_initialize_fabric_client_svc_from_env(self):
        """Initialize using env-provided service and verify headers and sub-client."""
        # Arrange & Act
        client = FabricAPIClient(api_version=API_VERSION)

        # Assert
        assert client.base_url == BASE_URL
        assert client.headers is not None
        assert "Authorization" in client.headers
        assert "Content-Type" in client.headers
        assert len(client.headers.get("Authorization", "")) > len("Bearer TOKEN")
        assert client.headers["Content-Type"] == "application/json"
        assert client.workspaces is not None

    def test_check_headers_auth(self):
        """Passes with valid token; raises PermissionError on missing/empty Authorization header."""
        # Arrange
        client = FabricAPIClient(api_version=API_VERSION)

        # Act / Assert: no exception with valid headers
        client.check_headers_auth()

        # Mutate headers to simulate an empty token and assert proper failure
        client._headers = {
            "Authorization": "Bearer ",
            "Content-Type": "application/json",
        }
        # Expect the client to reject missing token
        with pytest.raises(PermissionError, match="Authorization header is missing."):
            client.check_headers_auth()

    def test_global_fabric_import(self):
        """Lazily-imported global client has correct base URL, headers, and sub-client."""
        # Act
        from fabricengineer.api.fabric.client.fabric import fabric_client

        # Assert
        assert fabric_client().base_url == BASE_URL
        assert fabric_client().headers is not None
        assert "Authorization" in fabric_client().headers
        assert "Content-Type" in fabric_client().headers
        assert len(fabric_client().headers.get("Authorization", "")) > len("Bearer TOKEN")
        assert fabric_client().headers["Content-Type"] == "application/json"
        assert fabric_client().workspaces is not None

    def test_set_global_fabric_client(self, msf_svc: MicrosoftExtraSVC):
        """set_global_fabric_client replaces the globally referenced client instance."""
        from fabricengineer.api.fabric.client.fabric import fabric_client

        # Arrange: capture current instance identity
        assert fabric_client().headers is not None
        client_hash_before = fabric_client().__hash__()

        # Act: set a new global client using provided service
        set_global_fabric_client(msf_svc=msf_svc)

        # Assert: the referenced instance changed
        client_hash_after = fabric_client().__hash__()
        assert client_hash_before != client_hash_after

    def test_refresh_headers(self):
        """refresh_headers issues a new Authorization token; Content-Type remains JSON."""
        # Arrange
        client = FabricAPIClient(api_version=API_VERSION)
        headers_before = client.headers.copy()

        assert headers_before is not None
        assert len(headers_before.get("Authorization", "")) > len("Bearer TOKEN")
        assert headers_before.get("Content-Type", "") == "application/json"

        # Act
        client.refresh_headers()
        headers_after = client.headers.copy()

        # Assert
        assert headers_after != headers_before
        assert headers_after is not None
        assert len(headers_after.get("Authorization", "")) > len("Bearer TOKEN")
        assert headers_after.get("Content-Type", "") == "application/json"

    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/workspaces", "/workspaces"),
            ("/workspaces/123/items", "/workspaces/123/items"),
            ("workspaces", "/workspaces"),
            ("workspaces/123/items", "/workspaces/123/items"),
            ("", ""),
            (None, ""),
        ],
    )
    def test_prep_path(self, path, expected):
        """Normalize relative/absolute paths to a consistent API path shape used by the client."""
        # Arrange
        client = FabricAPIClient(api_version=API_VERSION)

        # Act / Assert
        assert client._prep_path(path) == expected

    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/workspaces", f"{BASE_URL}/workspaces"),
            ("/workspaces/123/items", f"{BASE_URL}/workspaces/123/items"),
            ("workspaces", f"{BASE_URL}/workspaces"),
            ("workspaces/123/items", f"{BASE_URL}/workspaces/123/items"),
            ("", f"{BASE_URL}"),
            (None, f"{BASE_URL}"),
        ],
    )
    def test_url(self, path, expected):
        """Construct absolute URLs by combining base_url and normalized path segments."""
        # Arrange
        client = FabricAPIClient(api_version=API_VERSION)

        # Act / Assert
        assert client._url(path) == expected

    def test_get_token_with_msf_svc(self, msf_svc: MicrosoftExtraSVC):
        """Retrieve a JWT with a provided MicrosoftExtraSVC; minimally validate shape."""
        # Arrange
        client = FabricAPIClient(msf_svc=msf_svc, api_version=API_VERSION)

        # Act
        token = client._get_token()

        # Assert
        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 0
        # Basic JWT structure contains dot separators
        assert "." in token

    def test_get_token_without_msf_svc_and_no_notebookutils(self):
        """Return empty token when no auth service is available and notebook utils are absent."""
        # Arrange: explicitly clear auth service
        client = FabricAPIClient(api_version=API_VERSION)
        client._msf_svc = None

        # Act
        token = client._get_token()

        # Assert
        assert client._msf_svc is None
        assert token == ""

    def test_get(self, fabric_client: FabricAPIClient, workspace_id):
        """GET a specific workspace; expect 200 and a JSON object payload."""
        # Act
        response = fabric_client.get(f"/workspaces/{workspace_id}")

        # Assert
        assert response is not None
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_get_list(self, fabric_client: FabricAPIClient):
        """GET all workspaces; expect 200, object payload with 'value' list."""
        # Act
        response = fabric_client.get("/workspaces")

        # Assert
        assert response is not None
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "value" in data
        assert isinstance(data["value"], list)

    def test_post_folders(self, fabric_client: FabricAPIClient, workspace_id):
        """POST a folder into a workspace and then delete it as cleanup."""
        # Arrange
        name = f"F_{uuid.uuid4().hex[:8]}"
        url = f"/workspaces/{workspace_id}/folders"
        payload = {"displayName": name}

        # Act: create
        response = fabric_client.post(url, payload=payload)

        # Assert: created
        assert response is not None
        assert response.status_code == 201
        data = response.json()
        assert isinstance(data, dict)

        # Cleanup: delete created folder
        folder_id = data.get("id")
        delete_url = f"{url}/{folder_id}"
        delete_response = fabric_client.delete(delete_url)

        assert delete_response is not None
        assert delete_response.status_code == 200

    def test_patch_folder(self, fabric_client: FabricAPIClient, workspace_id):
        """PATCH a folder's displayName; verify update and clean up created resource."""
        # Arrange: create a folder to update
        name = f"F_{uuid.uuid4().hex[:8]}"
        url = f"/workspaces/{workspace_id}/folders"
        payload = {"displayName": name}
        response = fabric_client.post(url, payload=payload)

        assert response.status_code == 201
        data = response.json()
        folder_id = data.get("id")

        # Act: patch/update the folder
        new_name = f"F_UPDATED_{uuid.uuid4().hex[:8]}"
        patch_url = f"{url}/{folder_id}"
        patch_payload = {"displayName": new_name}
        patch_response = fabric_client.patch(patch_url, payload=patch_payload)

        # Assert
        assert patch_response is not None
        assert patch_response.status_code == 200
        patch_data = patch_response.json()
        assert isinstance(patch_data, dict)
        assert patch_data.get("displayName") == new_name

        # Cleanup
        delete_response = fabric_client.delete(patch_url)
        assert delete_response.status_code == 200

    def test_delete_folder(self, fabric_client: FabricAPIClient, workspace_id):
        """DELETE a folder and verify subsequent GET returns 404 (not found)."""
        # Arrange: create a folder to delete
        name = f"F_{uuid.uuid4().hex[:8]}"
        url = f"/workspaces/{workspace_id}/folders"
        payload = {"displayName": name}
        response = fabric_client.post(url, payload=payload)

        assert response.status_code == 201
        data = response.json()
        folder_id = data.get("id")

        # Act: delete the folder
        delete_url = f"{url}/{folder_id}"
        delete_response = fabric_client.delete(delete_url)

        # Assert: deletion succeeded
        assert delete_response is not None
        assert delete_response.status_code == 200

        # And the resource should no longer be retrievable
        get_response = fabric_client.get(delete_url)
        assert get_response.status_code == 404
