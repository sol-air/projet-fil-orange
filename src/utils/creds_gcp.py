import os
import json
from dotenv import load_dotenv
from google.cloud import secretmanager, bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

DEFAULT_PROJECT_ID = "project-fil-orange"
load_dotenv()

def get_credentials():
    project_id = os.getenv("PROJECT_ID")
    secret_id = os.getenv("SECRET_ID")
    
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    
    sm_client = secretmanager.SecretManagerServiceClient()
    response = sm_client.access_secret_version(request={"name": name})
    payload = json.loads(response.payload.data.decode("UTF-8"))
    
    creds = service_account.Credentials.from_service_account_info(payload)
    return creds

def create_bq_client():
    project_id = os.getenv("PROJECT_ID")
    credentials = get_credentials()
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    print(f"Connecté à BigQuery sur le projet : {client.project}")
    return client

def get_api_key(secret_id, project_id=DEFAULT_PROJECT_ID, version_id="latest"):
    env_value = os.getenv(secret_id)
    if env_value:
        return env_value
    print(f"Secret '{secret_id}' non trouvé en ENV, tentative via Secret Manager ({version_id})...")
    
    try:
        client = secretmanager.SecretManagerServiceClient()
        
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        secret_value = response.payload.data.decode("UTF-8")
        return secret_value

    except NotFound:
        print(f"ERREUR : Le secret '{secret_id}' n'existe pas dans le projet {project_id}.")
        return None
    except Exception as e:
        print(f"ERREUR lors de la récupération du secret '{secret_id}' : {e}")
        return None