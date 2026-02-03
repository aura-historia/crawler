import os

from aura_historia_backend_api_client.client import Client

api_key = os.getenv("API_KEY")
headers = {"X-API-Key": api_key}
base_url = os.getenv("BACKEND_API_URL")

api_client = Client(base_url=base_url) if base_url else None
