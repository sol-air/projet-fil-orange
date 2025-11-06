from google.cloud import secretmanager
import json

SECRET_NAME = "projects/848698151781/secrets/sa-project-fil-orange"

client = secretmanager.SecretManagerServiceClient()

response = client.access_secret_version(name=f"{SECRET_NAME}/versions/latest")

secret_json = json.loads(response.payload.data.decode("utf-8"))

print(secret_json)
