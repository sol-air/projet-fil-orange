import os
import json
from dotenv import load_dotenv
from google.cloud import secretmanager, bigquery
from google.oauth2 import service_account

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