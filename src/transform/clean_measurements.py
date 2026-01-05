import sys
from pathlib import Path
from google.cloud import bigquery

# 1. Gestion des chemins
root_path = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(root_path))
from src.utils.creds_gcp import create_bq_client

# CONFIGURATION
PROJECT_ID = "project-fil-orange"
DATASET_ID = "load_air_quality"
BRONZE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_measurements"
SILVER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.clean_measurements"
ROLLING_WINDOW_DAYS = 3  

def run_silver_incremental(bq_client):
    """
    Met à jour la table Silver en mode incrémental strict :
    1. Supprime les données de la fenêtre glissante (3 jours).
    2. Insère les données fraîches/corrigées depuis Bronze.
    """
    print(f"--- Début de la mise à jour Silver (Fenêtre : {ROLLING_WINDOW_DAYS} jours) ---")

    print(f"Nettoyage des données récentes dans {SILVER_TABLE}...")
    delete_query = f"""
    DELETE FROM `{SILVER_TABLE}`
    WHERE date_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ROLLING_WINDOW_DAYS} DAY)
    """
    try:
        bq_client.query(delete_query).result()
        print("Nettoyage terminé.")
    except Exception as e:
        print(f"Erreur lors du DELETE (vérifiez que la table existe bien) : {e}")
        return

    print("Transformation et insertion des nouvelles données...")
    insert_query = f"""
    INSERT INTO `{SILVER_TABLE}` 
    (location_id, location_name, sensor_id, parameter, measurement_value, unit, date_utc, transformed_at)
    
    WITH raw_window AS (
        -- On ne lit que ce qui est nécessaire dans Bronze
        SELECT *
        FROM `{BRONZE_TABLE}`
        WHERE date_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ROLLING_WINDOW_DAYS} DAY)
    ),
    cleaned_data AS (
        SELECT
            SAFE_CAST(location_id AS INT64) as location_id,
            location_name,
            SAFE_CAST(sensor_id AS INT64) as sensor_id,
            parameter,
            SAFE_CAST(value AS FLOAT64) as measurement_value,
            unit,
            TIMESTAMP(date_utc) as date_utc,
            CURRENT_TIMESTAMP() as transformed_at
        FROM raw_window
        WHERE value IS NOT NULL
        AND SAFE_CAST(value AS FLOAT64) >= 0 -- Filtre qualité basique
    ),
    deduplicated AS (
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT 
                *,
                -- Dédoublonnage sur la fenêtre : on garde la version la plus récente
                ROW_NUMBER() OVER(
                    PARTITION BY sensor_id, date_utc 
                    ORDER BY transformed_at DESC
                ) as row_num
            FROM cleaned_data
        )
        WHERE row_num = 1
    )
    SELECT * FROM deduplicated
    """

    try:
        job = bq_client.query(insert_query)
        result = job.result()
        print(f"Succès : Mise à jour Silver terminée.")
    except Exception as e:
        print(f"Erreur lors de l'INSERT Silver : {e}")

def main():
    client = create_bq_client()
    run_silver_incremental(client)

if __name__ == "__main__":
    main()