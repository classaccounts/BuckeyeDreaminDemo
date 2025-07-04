import requests
from dotenv import load_dotenv
import os
load_dotenv()

class GoveSpendSearch:
    API_URL = "https://api.govspend.com/runSavedSearch"
    API_KEY = os.getenv("gs_api_key")  # Replace with a secure method

    @staticmethod
    def search(search_id):
        headers = {
            "accept": "application/json",
            "X-API-KEY": GoveSpendSearch.API_KEY,
            "Content-Type": "application/json"
        }
        payload = {
            "id": search_id,
            "page": 1,
            "include": [],
            "sortDir": "asc",
            "sortField": ""
        }

        response = requests.post(GoveSpendSearch.API_URL, json=payload, headers=headers)
        
        if response.status_code == 201:
            return response.json()
        else:
            return {"error": f"Request failed with status code {response.status_code}", "details": response.text}