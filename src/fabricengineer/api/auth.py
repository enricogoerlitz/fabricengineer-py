import requests

from dataclasses import dataclass

from fabricengineer.api.utils import check_http_response


@dataclass
class MicrosoftExtraSVC:
    tenant_id: str
    client_id: str
    client_secret: str

    def token(self) -> str:
        """
        Returns a token for the Microsoft Fabric API.
        This is a placeholder implementation and should be replaced with actual token retrieval logic.
        """
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
            "grant_type": "client_credentials",
        }

        resp = requests.post(token_url, data=data)
        check_http_response(resp)
        token = resp.json()["access_token"]

        return token
