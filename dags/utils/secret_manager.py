from google.cloud import secretmanager
import google

def access_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    try:
        response = client.access_secret_version(secret_name)
    except google.api_core.exceptions.GoogleAPICallError:
        return None
    return response.payload.data
