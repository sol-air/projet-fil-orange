import os
import requests
import pandas as pd
from datetime import datetime
# Importation depuis ton package utils
import sys
from pathlib import Path
root_path = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(root_path))
from src.utils.creds_gcp import create_bq_client
from google.cloud import bigquery


# def fetch_openaq_data_countries(api_key, id=None):
#     url = "https://api.openaq.org/v3/countries/{id}"
#     params = {
#         # "spatial": "location",
#         # "temporal": "day",
#         # "date_from": date_from,
#         # "date_to": datetime.now().strftime('%Y-%m-%d'),
#         # "format": "json",
#         # "limit": 1000
#         "id": id
#     }
#     headers = {"X-API-Key": api_key}
    
#     # print(f"Extraction OpenAQ : du {date_from} à aujourd'hui...")
#     try:
#         response = requests.get(url, params=params, headers=headers)
#         response.raise_for_status()
#         return response.json().get('results', [])
#     except Exception as e:
#         print(f"Erreur API : {e}")
#         return []

# def load_to_bronze(bq_client, data):
#     """
#     Charge les données dans la couche Bronze (données brutes).
#     """
#     # Ta destination spécifique
#     table_id = "project-fil-orange.load_air_quality.raw_averages"
    
#     df = pd.DataFrame(data)
#     if df.empty:
#         print("Aucune donnée à charger.")
#         return
        
#     # On s'assure que la colonne de partitionnement est au bon format
#     # df['date_utc'] = pd.to_datetime(df['date_from'])
    
#     # Configuration industrielle : Partitionnement journalier
#     job_config = bigquery.LoadJobConfig(
#         write_disposition="WRITE_APPEND",
#         time_partitioning=bigquery.TimePartitioning(
#             type_=bigquery.TimePartitioningType.DAY,
#             field="date_utc"
#         ),
#         autodetect=True
#     )

#     print(f"Chargement de {len(df)} lignes vers BigQuery...")
#     job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
#     job.result()
#     print(f"Succès : Données insérées dans {table_id}")

# def main():
#     client = create_bq_client()
#     api_key = os.getenv("OPEN_QUALITY_API_KEY")
    
#     if not api_key:
#         print("ERREUR : OPEN_QUALITY_API_KEY manquante dans l'environnement.")
#         return

#     raw_results = fetch_openaq_data_countries(api_key, id=22) # 22 = France
    
#     if raw_results:
#         load_to_bronze(client, raw_results)

# if __name__ == "__main__":
#     main()

def fetch_france_info(api_key):
    """
    Récupère les informations complètes du pays ID 22 (France).
    """
    url = "https://api.openaq.org/v3/countries/22"
    headers = {"X-API-Key": api_key}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        france_data = data.get('results', [])
        
        return france_data
        
    except Exception as e:
        print(f"Erreur API : {e}")
        return []

def load_to_data_countries(bq_client, data):
    """
    Charge les données dans la table 'data_countries' en remplaçant le contenu.
    """
    table_id = "project-fil-orange.load_air_quality.data_countries"
    
    df = pd.DataFrame(data)
    
    if df.empty:
        print("Aucune donnée trouvée.")
        return
    df['updated_at_bq'] = pd.Timestamp.now(tz='UTC')
    
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].astype(str)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", 
        autodetect=True
    )

    print(f"Mise à jour de la table {table_id} avec les infos de la France...")
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print("Succès ! La table contient maintenant les infos détaillées de l'ID 22.")

def main():
    client = create_bq_client()
    api_key = os.getenv("OPEN_QUALITY_API_KEY")
    
    if not api_key:
        print("ERREUR : Variable OPEN_QUALITY_API_KEY manquante.")
        return

    # 1. On récupère les infos de la France
    france_info = fetch_france_info(api_key)
    
    # 2. On charge proprement dans BigQuery
    if france_info:
        load_to_data_countries(client, france_info)

if __name__ == "__main__":
    main()