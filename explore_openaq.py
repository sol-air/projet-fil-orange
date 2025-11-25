import requests
import json

import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
# API OpenAQ v3
BASE_URL = "https://api.openaq.org/v3"


headers = {
    "X-API-Key": API_KEY
}

print("=" * 60)
print("EXPLORATION API OPENAQ v3")
print("=" * 60)

endpoints = [
    "/locations",
    "/parameters",
    "/countries",
    "/providers"
]

for endpoint in endpoints:
    print(f"\n{endpoint}")
    print("-" * 40)

    try:
        response = requests.get(
            f"{BASE_URL}{endpoint}",
            headers=headers,
            params={"limit": 5}
        )

        if response.status_code == 200:
            data = response.json()
            print(f"✓ Status: {response.status_code}")
            print(f"Structure: {type(data)}")

            if isinstance(data, dict):
                print(f"Clés: {list(data.keys())}")

                if "results" in data:
                    print(f"Nombre de résultats: {len(data['results'])}")
                    if len(data['results']) > 0:
                        print(f"\nPremier résultat:")
                        print(json.dumps(data['results'][0], indent=2)[:800])
        else:
            print(f"✗ Erreur: {response.status_code}")
            print(f"Message: {response.text[:200]}")

    except Exception as e:
        print(f"✗ Erreur: {e}")

print("\n" + "=" * 60)
print("EXPLORATION TERMINÉE")
print("=" * 60)