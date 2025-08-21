"""
Test module for FabricAPIWorkspaceClient.

Run tests with:
uv run pytest src/tests/api/fabric/client/test_workspace_client.py -v
"""
import uuid

from fabricengineer.api.fabric.client.fabric import FabricAPIClient
from fabricengineer.api.fabric.client.workspace import FabricAPIWorkspaceClient
from fabricengineer.api.auth import MicrosoftExtraSVC


def test_initialize_workspace_client(fabric_client: FabricAPIClient):
    """Test that workspace client is properly initialized."""
    workspace_client = fabric_client.workspaces

    assert isinstance(workspace_client, FabricAPIWorkspaceClient)
    assert workspace_client._client == fabric_client
    assert workspace_client._base_url == f"{fabric_client.base_url}/workspaces"


def test_workspace_client_from_fabric_client(msf_svc: MicrosoftExtraSVC):
    """Test workspace client initialization through fabric client."""
    fabric_client = FabricAPIClient(msf_svc=msf_svc, api_version="v1")
    workspace_client = fabric_client.workspaces

    assert isinstance(workspace_client, FabricAPIWorkspaceClient)
    assert workspace_client._client == fabric_client
    assert workspace_client._base_url == "https://api.fabric.microsoft.com/v1/workspaces"


def test_workspace_url_method_without_workspace_id():
    """Test the _url method when workspace_id is None."""
    fabric_client = FabricAPIClient(api_version="v1")
    workspace_client = fabric_client.workspaces

    # Test with no parameters
    assert workspace_client._url() == "https://api.fabric.microsoft.com/v1/workspaces"

    # Test with item_path only
    assert workspace_client._url(item_path="/items") == "https://api.fabric.microsoft.com/v1/workspaces/items"
    assert workspace_client._url(item_path="items") == "https://api.fabric.microsoft.com/v1/workspaces/items"

    # Test with empty item_path
    assert workspace_client._url(item_path="") == "https://api.fabric.microsoft.com/v1/workspaces"


def test_workspace_url_method_with_workspace_id():
    """Test the _url method when workspace_id is provided."""
    fabric_client = FabricAPIClient(api_version="v1")
    workspace_client = fabric_client.workspaces

    workspace_id = "12345678-1234-1234-1234-123456789012"

    # Test with workspace_id only
    assert workspace_client._url(workspace_id=workspace_id) == f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"

    # Test with workspace_id and item_path
    assert workspace_client._url(workspace_id=workspace_id, item_path="/items") == f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    assert workspace_client._url(workspace_id=workspace_id, item_path="items") == f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"

    # Test with workspace_id and various item paths
    assert workspace_client._url(workspace_id=workspace_id, item_path="/folders") == f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders"
    assert workspace_client._url(workspace_id=workspace_id, item_path="/items/123/definition") == f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/123/definition"


def test_workspace_get_all_workspaces(fabric_client: FabricAPIClient):
    """Test getting all workspaces."""
    workspace_client = fabric_client.workspaces

    response = workspace_client.get()

    assert response is not None
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert "value" in data
    assert isinstance(data["value"], list)


def test_workspace_get_specific_workspace(fabric_client: FabricAPIClient, workspace_id):
    """Test getting a specific workspace."""
    workspace_client = fabric_client.workspaces

    response = workspace_client.get(workspace_id=workspace_id)

    assert response is not None
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert data.get("id") == workspace_id


def test_workspace_get_workspace_items(fabric_client: FabricAPIClient, workspace_id):
    """Test getting items from a workspace."""
    workspace_client = fabric_client.workspaces

    response = workspace_client.get(workspace_id=workspace_id, item_path="/items")

    assert response is not None
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert "value" in data
    assert isinstance(data["value"], list)


def test_workspace_post_create_folder(fabric_client: FabricAPIClient, workspace_id):
    """Test creating a folder in a workspace."""
    workspace_client = fabric_client.workspaces

    folder_name = f"WS_Test_Folder_{uuid.uuid4().hex[:8]}"
    payload = {
        "displayName": folder_name
    }

    response = workspace_client.post(
        workspace_id=workspace_id,
        item_path="/folders",
        payload=payload
    )

    assert response is not None
    assert response.status_code == 201
    data = response.json()
    assert isinstance(data, dict)
    assert data.get("displayName") == folder_name

    # Cleanup - delete the created folder
    folder_id = data.get("id")
    delete_response = workspace_client.delete(
        workspace_id=workspace_id,
        item_path=f"/folders/{folder_id}"
    )
    assert delete_response.status_code == 200


def test_workspace_patch_update_folder(fabric_client: FabricAPIClient, workspace_id):
    """Test updating a folder in a workspace."""
    workspace_client = fabric_client.workspaces

    # First create a folder
    folder_name = f"WS_Test_Folder_{uuid.uuid4().hex[:8]}"
    create_payload = {
        "displayName": folder_name
    }

    create_response = workspace_client.post(
        workspace_id=workspace_id,
        item_path="/folders",
        payload=create_payload
    )

    assert create_response.status_code == 201
    folder_data = create_response.json()
    folder_id = folder_data.get("id")

    # Now update the folder
    new_folder_name = f"WS_Updated_Folder_{uuid.uuid4().hex[:8]}"
    update_payload = {
        "displayName": new_folder_name
    }

    patch_response = workspace_client.patch(
        workspace_id=workspace_id,
        item_path=f"/folders/{folder_id}",
        payload=update_payload
    )

    assert patch_response is not None
    assert patch_response.status_code == 200
    updated_data = patch_response.json()
    assert isinstance(updated_data, dict)
    assert updated_data.get("displayName") == new_folder_name

    # Cleanup - delete the folder
    delete_response = workspace_client.delete(
        workspace_id=workspace_id,
        item_path=f"/folders/{folder_id}"
    )
    assert delete_response.status_code == 200


def test_workspace_delete_folder(fabric_client: FabricAPIClient, workspace_id):
    """Test deleting a folder from a workspace."""
    workspace_client = fabric_client.workspaces

    # First create a folder to delete
    folder_name = f"WS_Delete_Test_{uuid.uuid4().hex[:8]}"
    create_payload = {
        "displayName": folder_name
    }

    create_response = workspace_client.post(
        workspace_id=workspace_id,
        item_path="/folders",
        payload=create_payload
    )

    assert create_response.status_code == 201
    folder_data = create_response.json()
    folder_id = folder_data.get("id")

    # Now delete the folder
    delete_response = workspace_client.delete(
        workspace_id=workspace_id,
        item_path=f"/folders/{folder_id}"
    )

    assert delete_response is not None
    assert delete_response.status_code == 200

    # Verify the folder is actually deleted by trying to get it
    get_response = workspace_client.get(
        workspace_id=workspace_id,
        item_path=f"/folders/{folder_id}"
    )
    assert get_response.status_code == 404  # Should not be found


def test_workspace_error_handling_invalid_workspace_id(fabric_client: FabricAPIClient):
    """Test error handling with invalid workspace ID."""
    workspace_client = fabric_client.workspaces

    invalid_workspace_id = "invalid-workspace-id"

    response = workspace_client.get(workspace_id=invalid_workspace_id)

    assert response is not None
    assert response.status_code in [400, 404]  # Bad Request or Not Found


def test_workspace_error_handling_unauthorized_workspace(fabric_client: FabricAPIClient):
    """Test error handling when accessing unauthorized workspace."""
    workspace_client = fabric_client.workspaces

    # Use a valid UUID format but non-existent workspace
    non_existent_workspace = "00000000-0000-0000-0000-000000000000"

    response = workspace_client.get(workspace_id=non_existent_workspace)

    assert response is not None
    assert response.status_code in [403, 404]  # Forbidden or Not Found


def test_workspace_headers_consistency(fabric_client: FabricAPIClient):
    """Test that workspace client uses the same headers as fabric client."""
    workspace_client = fabric_client.workspaces

    # Both should reference the same headers object
    assert workspace_client._client.headers is fabric_client.headers
    assert "Authorization" in workspace_client._client.headers
    assert "Content-Type" in workspace_client._client.headers
    assert workspace_client._client.headers["Content-Type"] == "application/json"


def test_workspace_base_url_consistency(fabric_client: FabricAPIClient):
    """Test that workspace client base URL is consistent with fabric client."""
    workspace_client = fabric_client.workspaces

    expected_base_url = f"{fabric_client.base_url}/workspaces"
    assert workspace_client._base_url == expected_base_url
    assert workspace_client._base_url == "https://api.fabric.microsoft.com/v1/workspaces"
