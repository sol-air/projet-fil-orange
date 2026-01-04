import sys
from pathlib import Path
from google.cloud import bigquery

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(root_path))
from src.utils.creds_gcp import create_bq_client

# CONFIGURATION
PROJECT_ID = "project-fil-orange"
DATASET_ID = "load_air_quality"
BRONZE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_measurements"
SILVER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.clean_measurements"

def run_silver_transformation(bq_client):
    """
    Exécute la transformation SQL pour créer/mettre à jour la table Silver.
    """
    
    sql_query = f"""
    CREATE OR REPLACE TABLE `{SILVER_TABLE}`
    PARTITION BY DATE(date_utc)
    CLUSTER BY location_name, parameter AS
    
    WITH cleaned_data AS (
        SELECT
            SAFE_CAST(location_id AS INT64) as location_id,
            location_name,
            SAFE_CAST(sensor_id AS INT64) as sensor_id,
            parameter,
            SAFE_CAST(value AS FLOAT64) as measurement_value,
            unit,
            TIMESTAMP(date_utc) as date_utc,
            CURRENT_TIMESTAMP() as transformed_at
        FROM `{BRONZE_TABLE}`
        WHERE value IS NOT NULL
          AND SAFE_CAST(value AS FLOAT64) >= 0 -- On élimine les erreurs capteurs
    )
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT 
            *,
            -- Dédoublonnage : on ne garde qu'une ligne par capteur et par date
            ROW_NUMBER() OVER(
                PARTITION BY sensor_id, date_utc 
                ORDER BY transformed_at DESC
            ) as row_num
        FROM cleaned_data
    )
    WHERE row_num = 1
    """

    print(f"Lancement de la transformation Silver vers {SILVER_TABLE}...")
    
    try:
        query_job = bq_client.query(sql_query)
        query_job.result() 
        print("Succès : La table Silver a été créée et dédoublonnée.")
    except Exception as e:
        print(f"Erreur lors de la transformation : {e}")

def main():
    client = create_bq_client()
    run_silver_transformation(client)

if __name__ == "__main__":
    main()