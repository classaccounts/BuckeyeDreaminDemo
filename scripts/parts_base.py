import requests
from dotenv import load_dotenv
import os

class PartsBase:
    def __init__(self):
        load_dotenv()
        self.token_url = "https://auth.partsbase.com/connect/token"
        self.search_url = "https://apiservices.partsbase.com/api/v1/search/RealTimeSearch"
        self.client_id = os.getenv('pb_client_id')
        self.client_secret = os.getenv('pb_client_secret')
        self.username = os.getenv('pb_username')
        self.password = os.getenv('pb_password')
        self.scope = "api openid"
        self.token = None

    def _authenticate(self):
        if self.token is None:
            data = {
                "grant_type": "password",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": self.scope,
                "username": self.username,
                "password": self.password
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            response = requests.post(self.token_url, data=data, headers=headers)
            response.raise_for_status()
            self.token = response.json()["access_token"]

    def get_live_qty(self, part_number):
        self._authenticate()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        params = {
            "FilterType": "PartNumber",
            "Filter": part_number,
            "XrefType": "UsMcrlXref",
            "Quantity": "10",
            "SortBy": "Seller",
            "ConditionCode": "SV,NS,NE,FN,RP,RD"
        }
        response = requests.get(self.search_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        total_quantity = 0
        for item in data.get("items", []):
            qty_str = item.get("quantity", "0")
            try:
                qty_val = int(qty_str)
            except ValueError:
                qty_val = 0
            total_quantity += qty_val
        return total_quantity
