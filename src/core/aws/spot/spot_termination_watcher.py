import asyncio
import json
from typing import Optional

import aiohttp

from src.core.utils.logger import logger

# EC2 Instance Metadata Service (IMDS) endpoints
# These are AWS standard endpoints and never change
IMDS_BASE_URL = "http://169.254.169.254"
IMDS_TOKEN_URL = f"{IMDS_BASE_URL}/latest/api/token"
IMDS_INSTANCE_ACTION_URL = f"{IMDS_BASE_URL}/latest/meta-data/spot/instance-action"

# Token TTL: 6 hours (21600 seconds)
IMDS_TOKEN_TTL_SECONDS = "21600"

# AWS recommends checking every 5 seconds
SPOT_CHECK_INTERVAL_SECONDS = 5.0


def signal_handler(signum: int, shutdown_event) -> None:
    """Signal handler that marks the worker as interrupted and re-queues
    the currently-processing SQS message (if any).
    """
    logger.info("Signal %s received. Finishing current task and shutting down.", signum)

    if shutdown_event is not None:
        try:
            shutdown_event.set()
        except Exception:
            logger.debug("Failed to signal shutdown_event to the loop")


async def get_metadata_token(session: aiohttp.ClientSession) -> Optional[str]:
    """
    Fetches a metadata token from the EC2 metadata service (IMDSv2).

    Args:
        session: aiohttp ClientSession for making HTTP requests.

    Returns:
        Metadata token string if successful, None otherwise.
    """
    headers = {"X-aws-ec2-metadata-token-ttl-seconds": IMDS_TOKEN_TTL_SECONDS}
    try:
        async with session.put(
            IMDS_TOKEN_URL, headers=headers, timeout=2
        ) as token_resp:
            if token_resp.status == 200:
                return await token_resp.text()
            else:
                logger.debug(f"Failed to get metadata token: {token_resp.status}")
                return None
    except Exception as e:
        logger.debug(f"Error fetching metadata token: {e}")
        return None


async def check_spot_termination_notice(
    session: aiohttp.ClientSession, event: asyncio.Event
) -> None:
    """
    Checks for a Spot Instance interruption notice and sets the event if found.

    AWS provides a 2-minute warning (except for hibernate which starts immediately).

    Args:
        session: aiohttp ClientSession for making HTTP requests.
        event: asyncio Event to set when interruption is detected.
    """
    try:
        token = await get_metadata_token(session)
        if not token:
            return

        headers = {"X-aws-ec2-metadata-token": token}
        async with session.get(
            IMDS_INSTANCE_ACTION_URL, headers=headers, timeout=2
        ) as resp:
            if resp.status == 200:
                try:
                    data = await resp.json()
                    action = data.get("action")
                    time = data.get("time")

                    # Handle all possible interruption types
                    if action in ["terminate", "stop", "hibernate"]:
                        if action == "hibernate":
                            logger.warning(
                                f"Spot hibernation starting immediately! Time: {time}"
                            )
                        else:
                            logger.warning(
                                f"Spot {action} imminent! 2-minute warning. Time: {time}"
                            )
                        event.set()
                    else:
                        logger.debug(f"Unknown instance-action: {action}")

                except json.JSONDecodeError:
                    logger.debug("Failed to decode instance-action JSON")
            elif resp.status == 404:
                # No interruption notice - this is normal
                pass
            else:
                logger.debug(f"Unexpected IMDS response status: {resp.status}")

    except asyncio.CancelledError:
        logger.info("Spot termination watcher cancelled.")
        raise
    except Exception as e:
        logger.debug(f"Error checking spot status: {e}")


async def watch_spot_termination(
    event: asyncio.Event, check_interval: float = SPOT_CHECK_INTERVAL_SECONDS
) -> None:
    """
    Poll for Spot Instance interruption notice using the modern 'instance-action' endpoint.

    AWS recommends checking every 5 seconds. The interruption notice provides a 2-minute
    warning (except for hibernation which starts immediately).

    Args:
        event: asyncio Event to set when interruption is detected.
        check_interval: Seconds between checks. Defaults to 5.0 (AWS recommendation).
    """
    logger.info(f"Starting Spot termination watcher (checking every {check_interval}s)")

    async with aiohttp.ClientSession() as session:
        while not event.is_set():
            try:
                # Wait for the check_interval or until the event is set.
                await asyncio.wait_for(event.wait(), timeout=check_interval)
            except asyncio.TimeoutError:
                # Timeout expired, time to check for the notice.
                if not event.is_set():
                    await check_spot_termination_notice(session, event)
            except asyncio.CancelledError:
                logger.info("Spot termination watcher task cancelled.")
                raise

        logger.info("Spot termination watcher finished.")
