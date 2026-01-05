import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import sys
from pathlib import Path

# 1. Gestion des chemins pour les imports locaux
root_path = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(root_path))
from src.utils.creds_gcp import create_bq_client

# CONFIGURATION
PROJECT_ID = "project-fil-orange"
DATASET_ID = "load_air_quality"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.raw_measurements"
COUNTRY_ID = 22  # France

def get_last_checkpoint(bq_client):
    """
    Récupère la date la plus récente dans BigQuery pour éviter les doublons.
    """
    query = f"SELECT MAX(date_utc) as last_date FROM `{TABLE_ID}`"
    try:
        query_job = bq_client.query(query)
        result = query_job.to_dataframe()
        last_date = result['last_date'].iloc[0]
        
        if pd.isna(last_date):
            return "2025-01-01"
        
        new_start = (last_date + timedelta(seconds=1)).strftime('%Y-%m-%d')
        return new_start
    except Exception:
        # Si la table n'existe pas encore ou erreur
        return "2025-01-01"

def fetch_active_locations(api_key):
    """
    Récupère les stations actives en France via l'endpoint V3 /locations.
    """
    url = "https://api.openaq.org/v3/locations"
    params = {
        "countries_id": COUNTRY_ID, 
        "limit": 100,
        "sort": "desc" 
    }
    headers = {"X-API-Key": api_key}
    
    res = requests.get(url, params=params, headers=headers)
    
    if res.status_code != 200:
        print(f"Erreur API Locations ({res.status_code}) : {res.text}")
        return []

    return res.json().get('results', [])

def fetch_sensor_measurements(api_key, sensor_id, date_from):
    """
    Récupère les mesures d'un CAPTEUR spécifique (Route V3 officielle).
    """
    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
    params = {
        "date_from": date_from,
        "limit": 1000
    }
    headers = {"X-API-Key": api_key}
    
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json().get('results', [])
    except Exception as e:
        print(f"Erreur sensor {sensor_id}: {e}")
        return []

def load_to_bigquery(bq_client, df):
    """
    Insère les données dans BigQuery en mode APPEND avec partitionnement.
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date_utc"
        ),
        autodetect=True
    )

    print(f"Chargement de {len(df)} lignes dans BigQuery...")
    job = bq_client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()

def main():
    # A. Initialisation
    client = create_bq_client()
    api_key = os.getenv("OPEN_QUALITY_API_KEY")
    
    if not api_key:
        print("ERREUR : Variable OPEN_QUALITY_API_KEY manquante.")
        return

    # B. Calcul du point de départ
    checkpoint = get_last_checkpoint(client)
    print(f"Début de l'extraction incrémentale depuis : {checkpoint}")

    # C. Récupération des stations
    locations = fetch_active_locations(api_key)
    if not locations:
        print("Aucune station trouvée.")
        return
    
    all_data = []
    
    for loc in locations:
        loc_id = loc['id']
        loc_name = loc['name']
        sensors = loc.get('sensors', [])
        
        print(f"Station : {loc_name} (ID: {loc_id}) | {len(sensors)} capteurs.")
        
        for sensor in sensors:
            sensor_id = sensor['id']
            param_info = sensor.get('parameter', {})
            parameter_name = param_info.get('name', 'unknown')
            unit = param_info.get('units', 'unknown')
            
            measurements = fetch_sensor_measurements(api_key, sensor_id, checkpoint)
            
            for m in measurements:
                period = m.get('period')
                if period and period.get('datetimeFrom'):
                    date_val = period['datetimeFrom']['utc']
                else:
                    continue 

                all_data.append({
                    "location_id": loc_id,
                    "location_name": loc_name,
                    "sensor_id": sensor_id,
                    "parameter": parameter_name,
                    "value": m['value'],
                    "unit": unit,
                    "date_utc": date_val
                })

    if all_data:
        df = pd.DataFrame(all_data)
        df['date_utc'] = pd.to_datetime(df['date_utc'])
        
        load_to_bigquery(client, df)
        print(f"Succès : Pipeline terminé. {len(df)} nouvelles lignes insérées.")
    else:
        print("Aucune nouvelle donnée disponible à partir de cette date.")

if __name__ == "__main__":
    main()