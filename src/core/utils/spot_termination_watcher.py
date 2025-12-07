import asyncio
import json
import os
from typing import Optional

import aiohttp

from src.core.utils.logger import logger


def signal_handler(signum: int, shutdown_event) -> None:
    """Signal handler that marks the worker as interrupted and re-queues
    the currently-processing SQS message (if any).

    The `frame` argument is required by the Python `signal` API even if
    unused.
    """
    logger.info("Signal %s received. Finishing current task and shutting down.", signum)

    if shutdown_event is not None:
        try:
            shutdown_event.set()
        except Exception:
            logger.debug("Failed to signal shutdown_event to the loop")


async def _get_metadata_token(session: aiohttp.ClientSession) -> Optional[str]:
    """Fetches a metadata token from the EC2 metadata service."""
    token_url = os.getenv("EC2_TOKEN_URL")
    headers = {"X-aws-ec2-metadata-token-ttl-seconds": "21600"}
    try:
        async with session.put(token_url, headers=headers, timeout=2) as token_resp:
            if token_resp.status == 200:
                return await token_resp.text()
            else:
                logger.debug(f"Failed to get metadata token: {token_resp.status}")
                return None
    except Exception as e:
        logger.debug(f"Error fetching metadata token: {e}")
        return None


async def _check_spot_termination_notice(
    session: aiohttp.ClientSession, event: asyncio.Event
):
    """Checks for a spot termination notice and sets the event if found."""
    metadata_url = os.getenv("EC2_METADATA_URL")
    try:
        token = await _get_metadata_token(session)
        if not token:
            return

        headers = {"X-aws-ec2-metadata-token": token}
        async with session.get(metadata_url, headers=headers, timeout=2) as resp:
            if resp.status == 200:
                try:
                    data = await resp.json()
                    action = data.get("action")
                    if action in ["terminate", "stop"]:
                        logger.warning(
                            f"Spot {action} imminent! Time: {data.get('time')}"
                        )
                        event.set()
                except json.JSONDecodeError:
                    logger.debug("Failed to decode instance-action JSON")
    except asyncio.CancelledError:
        logger.info("Spot termination watcher cancelled.")
        raise
    except Exception as e:
        logger.debug(f"Error checking spot status: {e}")


async def watch_spot_termination(event: asyncio.Event, check_interval: int = 5) -> None:
    """
    Poll for spot termination notice using the modern 'instance-action' endpoint.
    """
    async with aiohttp.ClientSession() as session:
        while not event.is_set():
            try:
                # Wait for the check_interval or until the event is set.
                await asyncio.wait_for(event.wait(), timeout=check_interval)
            except asyncio.TimeoutError:
                # Timeout expired, time to check for the notice.
                if not event.is_set():
                    await _check_spot_termination_notice(session, event)
            except asyncio.CancelledError:
                logger.info("Spot termination watcher task cancelled.")
                raise

        logger.info("Spot termination watcher finished.")
