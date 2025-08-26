"""


Run tests: uv run pytest src/tests/api/fabric/test_auth.py -v
"""
import os
from fabricengineer.api.auth import MicrosoftExtraSVC


def test_create_microsoft_extra_svc():
    svc = MicrosoftExtraSVC(
        tenant_id=os.getenv("MICROSOFT_TENANT_ID"),
        client_id=os.getenv("SVC_MICROSOFT_FABRIC_CLIENT_ID"),
        client_secret=os.getenv("SVC_MICROSOFT_FABRIC_SECRET_VALUE")
    )

    assert len(svc.tenant_id) == 36
    assert len(svc.client_id) == 36
    assert len(svc.client_secret) == 40


def test_get_token():
    svc = MicrosoftExtraSVC(
        tenant_id=os.getenv("MICROSOFT_TENANT_ID"),
        client_id=os.getenv("SVC_MICROSOFT_FABRIC_CLIENT_ID"),
        client_secret=os.getenv("SVC_MICROSOFT_FABRIC_SECRET_VALUE")
    )
    token = svc.token()
    assert token is not None
    assert isinstance(token, str)
    assert len(token) > 0
