"""
Script pour vérifier que tous les accès fonctionnent
"""

print("=" * 50)
print("VÉRIFICATION DES ACCÈS")
print("=" * 50)

# 1. Vérifier Git
print("\n1. Git...")
try:
    import git

    repo = git.Repo('.')
    print(f"   ✓ Branche actuelle : {repo.active_branch}")
except Exception as e:
    print(f"   ✗ Erreur Git : {e}")
    print("   → Installer avec: pip install gitpython")

# 2. Vérifier les packages Python
print("\n2. Packages Python...")
try:
    import pandas as pd

    print(f"   ✓ Pandas : OK")
except:
    print("   ✗ Pandas manquant")
    print("   → Installer avec: pip install pandas")

try:
    from google.cloud import bigquery

    print(f"   ✓ BigQuery : OK")
except:
    print("   ✗ BigQuery manquant")
    print("   → Installer avec: pip install google-cloud-bigquery")

try:
    import requests

    print(f"   ✓ Requests : OK")
except:
    print("   ✗ Requests manquant")
    print("   → Installer avec: pip install requests")

# 3. Vérifier l'accès à BigQuery
print("\n3. Connexion BigQuery...")
try:
    from google.cloud import bigquery

    client = bigquery.Client()
    project = client.project
    print(f"   ✓ Connecté au projet : {project}")

    datasets = list(client.list_datasets())
    print(f"   ✓ Datasets accessibles : {len(datasets)}")

except Exception as e:
    print(f"   ✗ Erreur BigQuery : {e}")
    print("   → Vérifie ton authentification Google Cloud")

print("\n" + "=" * 50)
print("FIN DE LA VÉRIFICATION")
print("=" * 50)