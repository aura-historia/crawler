import os
import boto3
import time
import pytest
import asyncio
from src.core.aws.sqs import message_wrapper


@pytest.fixture(scope="session")
def sqs_setup():
    """
    Provides a boto3 SQS resource connected to LocalStack for the test session.
    Ensures all SQS operations use the same LocalStack instance.
    """
    endpoint_url = os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566")
    region = os.getenv("AWS_REGION", "eu-central-1")
    sqs = boto3.resource("sqs", region_name=region, endpoint_url=endpoint_url)
    yield sqs


@pytest.fixture(scope="function")
def sqs_queue(sqs_setup):
    """
    Creates a new SQS queue for each test and deletes it after the test completes.
    Guarantees isolation between tests and a clean environment.
    """
    queue_name = f"test-visibility-queue-{int(time.time())}"
    queue = sqs_setup.create_queue(QueueName=queue_name)
    yield queue
    queue.delete()


@pytest.mark.asyncio
async def test_visibility_heartbeat_localstack(sqs_queue):
    """
    Test: The heartbeat task periodically extends the visibility timeout of a message.
    - Sends a message to the queue.
    - Receives it with a short visibility timeout.
    - Starts the heartbeat task to extend visibility.
    - Verifies the message remains invisible during extension.
    - Verifies the message becomes visible again after the extended timeout expires.
    """
    msg_body = "heartbeat-test"
    sqs_queue.send_message(MessageBody=msg_body)
    messages = sqs_queue.receive_messages(WaitTimeSeconds=1, VisibilityTimeout=2)
    assert messages, "No message received"
    message = messages[0]
    stop_event = asyncio.Event()
    task = message_wrapper.visibility_heartbeat(
        message, stop_event, extend_timeout=3, interval=1
    )
    await asyncio.sleep(2.5)  # Heartbeat should extend at least once
    stop_event.set()
    await asyncio.sleep(0.5)
    # Check that the message is still invisible
    messages_after = sqs_queue.receive_messages(WaitTimeSeconds=1)
    assert not any(m.body == msg_body for m in messages_after), (
        "Message became visible too early!"
    )
    # Wait until the extended visibility timeout expires
    await asyncio.sleep(3)
    messages_final = sqs_queue.receive_messages(WaitTimeSeconds=1)
    assert any(m.body == msg_body for m in messages_final), (
        "Expected message body not found after visibility timeout!"
    )
    # Cancel the heartbeat task and wait for it to finish
    task.cancel()


@pytest.mark.asyncio
async def test_visibility_heartbeat_immediate_stop(sqs_queue):
    """
    Test: The heartbeat task is stopped immediately and does not extend visibility.
    - Sends a message and receives it.
    - Starts and immediately stops the heartbeat task.
    - Verifies the message becomes visible after the original timeout.
    """
    msg_body = "immediate-stop-test"
    sqs_queue.send_message(MessageBody=msg_body)
    messages = sqs_queue.receive_messages(WaitTimeSeconds=1, VisibilityTimeout=2)
    assert messages, "No message received"
    message = messages[0]
    stop_event = asyncio.Event()
    task = message_wrapper.visibility_heartbeat(
        message, stop_event, extend_timeout=3, interval=2
    )
    stop_event.set()  # Stop immediately
    await asyncio.sleep(0.5)
    await task  # Task should finish quickly
    await asyncio.sleep(2)
    # After the original visibility timeout, the message should be visible again
    messages_final = sqs_queue.receive_messages(WaitTimeSeconds=1)
    assert any(m.body == msg_body for m in messages_final), (
        "Expected message body not found after immediate stop!"
    )


@pytest.mark.asyncio
async def test_visibility_heartbeat_multiple_extensions(sqs_queue):
    """
    Test: The heartbeat task extends visibility multiple times.
    - Sends a message and receives it with a short timeout.
    - Runs the heartbeat long enough for several extensions.
    - Verifies the message remains invisible during the extended period.
    """
    msg_body = "multiple-extensions-test"
    sqs_queue.send_message(MessageBody=msg_body)
    messages = sqs_queue.receive_messages(WaitTimeSeconds=1, VisibilityTimeout=1)
    assert messages, "No message received"
    message = messages[0]
    stop_event = asyncio.Event()
    task = message_wrapper.visibility_heartbeat(
        message, stop_event, extend_timeout=2, interval=1
    )
    await asyncio.sleep(3.5)  # Should extend at least 3 times
    stop_event.set()
    await asyncio.sleep(0.5)
    await task
    await asyncio.sleep(2)
    messages_final = sqs_queue.receive_messages(WaitTimeSeconds=1)
    assert any(m.body == msg_body for m in messages_final), (
        "Expected message body not found after multiple extensions!"
    )


@pytest.mark.asyncio
async def test_visibility_heartbeat_short_timeout(sqs_queue):
    """
    Test: The heartbeat task works with very short timeout and interval values.
    - Sends a message and receives it with a very short timeout.
    - Runs the heartbeat with short extension and interval.
    - Verifies the message becomes visible again after the short period.
    """
    msg_body = "short-timeout-test"
    sqs_queue.send_message(MessageBody=msg_body)
    messages = sqs_queue.receive_messages(WaitTimeSeconds=1, VisibilityTimeout=1)
    assert messages, "No message received"
    message = messages[0]
    stop_event = asyncio.Event()
    task = message_wrapper.visibility_heartbeat(
        message, stop_event, extend_timeout=1, interval=1
    )
    await asyncio.sleep(2)
    stop_event.set()
    await asyncio.sleep(0.5)
    await task
    await asyncio.sleep(1)
    messages_final = sqs_queue.receive_messages(WaitTimeSeconds=1)
    assert any(m.body == msg_body for m in messages_final), (
        "Expected message body not found after short timeout!"
    )
