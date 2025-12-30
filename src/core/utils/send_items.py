import os
from aiohttp import ClientSession

from src.core.utils.logger import logger
from src.core.utils.network import resilient_http_request
from dotenv import load_dotenv

load_dotenv()


async def send_items(items):
    api_url = os.getenv("AWS_API_URL")
    if not api_url:
        print("ERROR: AWS_API_URL environment variable is not set.")
        return
    headers = {"Content-Type": "application/json"}
    async with ClientSession() as session:
        response = await resilient_http_request(
            api_url,
            session,
            method="PUT",
            json_data={"items": items},
            headers=headers,
            timeout_seconds=10,
            retry_attempts=3,
        )

    logger.info("Response from API:", response)
