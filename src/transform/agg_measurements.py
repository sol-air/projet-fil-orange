import sys
from pathlib import Path
from google.cloud import bigquery

# Gestion des chemins pour les imports locaux
root_path = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(root_path))
from src.utils.creds_gcp import create_bq_client

# CONFIGURATION
PROJECT_ID = "project-fil-orange"
DATASET_ID = "load_air_quality"
SILVER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.clean_measurements"
GOLD_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_daily_air_quality"

def run_gold_transformation(bq_client):
    """
    Crée la table Gold agrégée par ville et par jour.
    """
    
    # Correction : On a supprimé le ORDER BY à la fin
    sql_query = f"""
    CREATE OR REPLACE TABLE `{GOLD_TABLE}`
    CLUSTER BY location_name, parameter AS
    
    SELECT
        DATE(date_utc) as date_day,
        location_id,
        location_name,
        parameter,
        unit,
        ROUND(AVG(measurement_value), 2) as avg_value,
        ROUND(MAX(measurement_value), 2) as max_value,
        COUNT(*) as nb_measurements,
        -- Indicateur de qualité simplifié
        CASE 
            WHEN parameter = 'pm25' AND AVG(measurement_value) > 25 THEN 'Mauvaise'
            WHEN parameter = 'pm25' AND AVG(measurement_value) > 10 THEN 'Moyenne'
            WHEN parameter = 'pm25' THEN 'Bonne'
            ELSE 'N/A'
        END as simple_aqi_status,
        CURRENT_TIMESTAMP() as calculated_at
    FROM `{SILVER_TABLE}`
    GROUP BY ALL
    """

    print(f"Lancement de la transformation Gold vers {GOLD_TABLE}...")
    
    try:
        query_job = bq_client.query(sql_query)
        query_job.result()
        print(f"Succès : La table Gold a été générée.")
    except Exception as e:
        print(f"Erreur lors de la transformation Gold : {e}")

def main():
    client = create_bq_client()
    run_gold_transformation(client)

if __name__ == "__main__":
    main()