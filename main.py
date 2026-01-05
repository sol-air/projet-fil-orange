import sys
import logging
from pathlib import Path

root_path = Path(__file__).resolve().parent
sys.path.append(str(root_path))

try:
    from src.load.load_measurements import main as run_bronze_etl
    from src.transform.clean_measurements import main as run_silver_etl
except ImportError as e:
    print(f"ERREUR D'IMPORT : Vérifiez la structure des dossiers. {e}")
    sys.exit(1)

# CONFIG LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Orchestrateur")

def run_pipeline():
    """
    Orchestre l'exécution séquentielle : Bronze -> Silver.
    Arrête tout si une étape échoue.
    """
    logger.info("🚀 DÉMARRAGE DU PIPELINE COMPLET")

    try:
        logger.info("[1/2] Lancement de l'ETL Bronze...")
        run_bronze_etl()
        logger.info("[1/2] ETL Bronze terminé avec succès.")

        logger.info("[2/2] Lancement de l'ETL Silver...")
        run_silver_etl()
        logger.info("[2/2] ETL Silver terminé avec succès.")
    except Exception as e:
        logger.error(f"FAIL : {e}")
        sys.exit(1)

    logger.info("End Scheduler")

if __name__ == "__main__":
    run_pipeline()