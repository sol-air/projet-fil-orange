import requests
import pandas as pd

url = "https://api.openaq.org/v2/measurements"

params = {
    "city": "Paris",          
    "limit": 100,             
    "parameter": ["pm25","pm10"],
    "date_from": "2025-01-01",
    "date_to": "2025-01-05",
    "order_by": "datetime",
    "sort": "asc"
}

response = requests.get(url, params=params)
data = response.json()

print(data)

