import requests
import json
from google.cloud import secretmanager, bigquery
from google.oauth2 import service_account
import pandas as pd

API_KEY = "84275f0224af44cae3b78e6a3601ff4af326322d91b77c7fa1c326e5f9b1ceac"
ENDPOINT_LOCATION = "https://api.openaq.org/v3/locations"
HEADERS = {"X-API-Key": API_KEY}
SECRET_NAME = "projects/848698151781/secrets/sa-project-fil-orange"

def creds(secret_name: str):
    """
    Récupère les accès du compte de service et crée un client BigQuery.
    """
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=f"{secret_name}/versions/latest")
    secret_json = json.loads(response.payload.data.decode("utf-8"))

    credentials = service_account.Credentials.from_service_account_info(secret_json)
    bq_client = bigquery.Client(credentials=credentials, project=secret_json["project_id"])
    return bq_client

def get_data(limit=1000):
    """
    Récupère les données depuis l'API OpenAQ.
    """
    r = requests.get(ENDPOINT_LOCATION, headers=HEADERS, params={"limit": limit})
    return r.json().get("results", [])

def to_dataframe(data):
    """
    Transforme les données JSON en DataFrame compatible BigQuery.
    Nettoie les noms de colonnes et convertit listes/dicts en chaînes.
    """
    df = pd.json_normalize(data)

    # Nettoyage des colonnes pour BigQuery : remplace . par _ et supprime caractères invalides
    df.columns = [
        col.replace(".", "_").replace("-", "_") for col in df.columns
    ]

    # Convertir les colonnes listes ou dict en JSON string
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: ", ".join(x) if isinstance(x, list) and all(isinstance(i, str) for i in x)
            else (json.dumps(x) if isinstance(x, (list, dict)) else x)
        )
    
    return df

def send_to_bigquery(bq_client, df, dataset_id="sandbox_yassine", table_id="locations"):
    """
    Envoie un DataFrame dans BigQuery.
    """
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"
    job = bq_client.load_table_from_dataframe(df, table_ref)
    job.result()  # Attendre la fin du job
    print(f"{job.output_rows} lignes insérées dans {table_ref}")

if __name__ == "__main__":
    # Crée le client BigQuery
    bq_client = creds(SECRET_NAME)
    
    # Récupère les données
    data = get_data(limit=1000)
    
    # Transforme en DataFrame
    df = to_dataframe(data)
    print("Aperçu des données :")
    print(df.head())
    
    # Envoie dans BigQuery
    send_to_bigquery(bq_client, df)
