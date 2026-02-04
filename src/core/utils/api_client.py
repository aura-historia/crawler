import os

from dotenv import load_dotenv
from aura_historia_backend_api_client.client import Client

load_dotenv()

api_key = os.getenv("API_KEY")
headers = {"X-API-Key": api_key}
base_url = os.getenv("BACKEND_API_URL")

api_client = Client(base_url=base_url) if base_url else None
