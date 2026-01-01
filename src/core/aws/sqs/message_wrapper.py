import asyncio
import json
import logging
import os
from typing import Optional, Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
sqs = boto3.resource(
    "sqs",
    region_name=os.getenv("AWS_REGION"),
    endpoint_url=os.getenv("SQS_ENDPOINT_URL"),
)


def send_message(
    queue: Any, message_body: str, message_attributes: Optional[dict] = None
) -> dict:
    """
    Send a message to an Amazon SQS queue.

    Parameters:
        queue (Any): The queue that receives the message.
        message_body (str): The body text of the message.
        message_attributes (dict, optional): Custom attributes of the message as key-value pairs.

    Returns:
        dict: The response from SQS that contains the assigned message ID.

    Raises:
        ClientError: If sending the message fails.
    """
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body, MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response


def send_messages(queue: Any, messages: list[dict]) -> dict:
    """
    Send a batch of messages in a single request to an SQS queue.

    Parameters:
        queue (Any): The queue to receive the messages.
        messages (list[dict]): List of messages, each with 'body' and 'attributes'.

    Returns:
        dict: The response from SQS with lists of successful and failed messages.

    Raises:
        ClientError: If sending the batch fails.
    """
    try:
        entries = [
            {
                "Id": str(ind),
                "MessageBody": msg["body"],
                "MessageAttributes": msg["attributes"],
            }
            for ind, msg in enumerate(messages)
        ]
        response = queue.send_messages(Entries=entries)
        if "Successful" in response:
            for msg_meta in response["Successful"]:
                logger.info(
                    "Message sent: %s: %s",
                    msg_meta["MessageId"],
                    messages[int(msg_meta["Id"])]["body"],
                )
        if "Failed" in response:
            for msg_meta in response["Failed"]:
                logger.warning(
                    "Failed to send: %s: %s",
                    msg_meta["MessageId"],
                    messages[int(msg_meta["Id"])]["body"],
                )
    except ClientError as error:
        logger.exception("Send messages failed to queue: %s", queue)
        raise error
    else:
        return response


def receive_messages(queue: Any, max_number: int, wait_time: int) -> list:
    """
    Receive a batch of messages in a single request from an SQS queue.

    Parameters:
        queue (Any): The queue from which to receive messages.
        max_number (int): Maximum number of messages to receive.
        wait_time (int): Maximum time to wait (seconds) before returning.

    Returns:
        list: List of Message objects received.

    Raises:
        ClientError: If receiving messages fails.
    """
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time,
        )
        for msg in messages:
            logger.info("Received message: %s: %s", msg.message_id, msg.body)
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        return messages


def delete_message(message: Any) -> None:
    """
    Delete a message from a queue after processing.

    Parameters:
        message (Any): The message to delete.

    Returns:
        None

    Raises:
        ClientError: If deleting the message fails.
    """
    try:
        message.delete()
        logger.info("Deleted message: %s", message.message_id)
    except ClientError as error:
        logger.exception("Couldn't delete message: %s", message.message_id)
        raise error


def delete_messages(queue: Any, messages: list) -> dict:
    """
    Delete a batch of messages from a queue in a single request.

    Parameters:
        queue (Any): The queue from which to delete the messages.
        messages (list): List of messages to delete.

    Returns:
        dict: The response from SQS with lists of successful and failed deletions.

    Raises:
        ClientError: If deleting the batch fails.
    """
    try:
        entries = [
            {"Id": str(ind), "ReceiptHandle": msg.receipt_handle}
            for ind, msg in enumerate(messages)
        ]
        response = queue.delete_messages(Entries=entries)
        if "Successful" in response:
            for msg_meta in response["Successful"]:
                logger.info("Deleted %s", messages[int(msg_meta["Id"])].receipt_handle)
        if "Failed" in response:
            for msg_meta in response["Failed"]:
                logger.warning(
                    "Could not delete %s", messages[int(msg_meta["Id"])].receipt_handle
                )
    except ClientError as error:
        logger.exception("Couldn't delete messages from queue %s", queue)
        raise error
    else:
        return response


def parse_message_body(message: Any) -> tuple[Optional[str], Optional[str]]:
    """
    Parse the JSON body of an SQS message produced by this project.

    Parameters:
        message (Any): SQS message object with a `body` attribute.

    Returns:
        tuple[Optional[str], Optional[str]]: Tuple of domain and next_url, or None if absent.

    Raises:
        json.JSONDecodeError: If the message body is not valid JSON.
        TypeError, AttributeError: If the message object is malformed.
    """
    try:
        body = json.loads(getattr(message, "body", "{}"))
        return body.get("domain"), body.get("next")
    except json.JSONDecodeError as e:
        logger.exception(
            "Failed to parse message body (invalid JSON): %s; body=%s",
            e,
            getattr(message, "body", None),
        )
        return None, None
    except (TypeError, AttributeError) as e:
        logger.exception(
            "Unexpected error parsing message body (type/attr issue): %s; body=%s",
            e,
            getattr(message, "body", None),
        )
        return None, None


def visibility_heartbeat(
    message: Any,
    stop_event: asyncio.Event,
    extend_timeout: int = 600,
    interval: int = 300,
) -> asyncio.Task:
    """
    Periodically extends the visibility timeout of an SQS message until a stop event is set.

    Parameters:
        message (Any): The SQS message object whose visibility timeout will be extended.
        stop_event (asyncio.Event): Event to signal when to stop extending visibility.
        extend_timeout (int, optional): Timeout (in seconds) to set on each extension. Default is 600 (10 min).
        interval (int, optional): Interval (in seconds) between extensions. Default is 300 (5 min).

    Returns:
        asyncio.Task: The asyncio task running the heartbeat loop.
    """

    async def _heartbeat():
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    break
                except asyncio.TimeoutError:
                    try:
                        await asyncio.to_thread(
                            message.change_visibility,
                            VisibilityTimeout=extend_timeout,
                        )
                        logger.debug(
                            "Extended visibility for message %s",
                            getattr(message, "message_id", None),
                        )
                    except Exception as e:
                        logger.error("Failed to extend visibility: %s", e)
        except asyncio.CancelledError:
            logger.debug(
                "Heartbeat task for message %s was cancelled.",
                getattr(message, "message_id", None),
            )
            raise

    task = asyncio.create_task(_heartbeat())
    return task
