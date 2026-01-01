from __future__ import annotations

import asyncio
from typing import Any, Callable, Awaitable

from src.core.utils.logger import logger
from src.core.aws.sqs.message_wrapper import receive_messages


async def handle_stolen_message(worker_id: int, fetch_task: asyncio.Task) -> None:
    """Handle messages caught in race condition between fetching and shutdown.

    When a shutdown signal arrives while a worker is fetching a message,
    the message may already be pulled from SQS. This function ensures
    the message is released back to the queue with a timeout that prevents
    other local workers from grabbing it during process shutdown.

    Args:
        worker_id (int): Identifier of the worker handling the message.
        fetch_task (asyncio.Task): The pending or completed fetch task.
    """
    if not fetch_task.done() or fetch_task.cancelled():
        return

    try:
        messages = fetch_task.result()
        if messages:
            logger.debug(
                f"Worker-{worker_id} releasing 'stolen' message (120s timeout)."
            )
            # 120s ensures other local workers won't grab it during process exit
            await asyncio.to_thread(
                messages[0].change_visibility, VisibilityTimeout=120
            )
    except Exception:
        logger.warning(f"Worker-{worker_id} failed to release stolen message.")


async def generic_worker(
    worker_id: int,
    queue: Any,
    shutdown_event: asyncio.Event,
    message_handler: Callable[[Any], Awaitable[None]],
    max_messages: int = 1,
    wait_time: int = 20,
) -> None:
    """Generic worker loop for processing SQS messages.

    Continuously fetches and processes messages from an SQS queue until
    shutdown is signaled. Implements graceful shutdown with message
    stealing prevention.

    Args:
        worker_id (int): Unique identifier for this worker instance.
        queue (Any): SQS queue object to poll messages from.
        shutdown_event (asyncio.Event): Event signaling graceful shutdown.
        message_handler (Callable): Async function to process each message.
            Should accept the message object as its first parameter.
        max_messages (int): Maximum number of messages to fetch per poll.
            Defaults to 1.
        wait_time (int): Long polling wait time in seconds. Defaults to 20.
    """
    logger.info(f"Worker-{worker_id} started.")

    while not shutdown_event.is_set():
        fetch_task = asyncio.create_task(
            asyncio.to_thread(receive_messages, queue, max_messages, wait_time)
        )
        shutdown_waiter = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            [fetch_task, shutdown_waiter], return_when=asyncio.FIRST_COMPLETED
        )

        for t in pending:
            t.cancel()

        # Case 1 - Shutdown Triggered
        if shutdown_waiter in done:
            await handle_stolen_message(worker_id, fetch_task)
            break

        # Case 2 - Message Received Normally
        try:
            messages = await fetch_task
            if messages:
                await message_handler(messages[0])
        except Exception as e:
            logger.exception(f"Worker-{worker_id} error: {e}")

    logger.info(f"Worker-{worker_id} shut down.")
