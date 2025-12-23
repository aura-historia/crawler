import asyncio

import pytest
from unittest.mock import MagicMock
from src.core.aws.sqs import message_wrapper
from botocore.exceptions import ClientError


@pytest.fixture
def mock_sqs_queue():
    """Fixture for a mocked SQS queue."""
    queue = MagicMock()
    queue.send_message.return_value = {"MessageId": "123"}
    queue.send_messages.return_value = {
        "Successful": [{"Id": "0", "MessageId": "abc"}],
        "Failed": [],
    }
    mock_message = MagicMock()
    mock_message.message_id = "456"
    mock_message.body = "test body"
    mock_message.receipt_handle = "handle_123"
    queue.receive_messages.return_value = [mock_message]
    queue.delete_messages.return_value = {"Successful": [{"Id": "0"}], "Failed": []}
    return queue


def test_send_message(mock_sqs_queue):
    """Test sending a single message."""
    response = message_wrapper.send_message(mock_sqs_queue, "test body")
    mock_sqs_queue.send_message.assert_called_once_with(
        MessageBody="test body", MessageAttributes={}
    )
    assert response == {"MessageId": "123"}


def test_send_message_with_attributes(mock_sqs_queue):
    """Test sending a single message with attributes."""
    attributes = {"Attribute1": {"DataType": "String", "StringValue": "Value1"}}
    message_wrapper.send_message(mock_sqs_queue, "test body", attributes)
    mock_sqs_queue.send_message.assert_called_once_with(
        MessageBody="test body", MessageAttributes=attributes
    )


def test_send_message_client_error(mock_sqs_queue):
    """Test ClientError on sending a message."""
    mock_sqs_queue.send_message.side_effect = ClientError({}, "SendMessage")
    with pytest.raises(ClientError):
        message_wrapper.send_message(mock_sqs_queue, "test body")


def test_send_messages(mock_sqs_queue):
    """Test sending a batch of messages."""
    messages = [
        {"body": "body1", "attributes": {}},
        {"body": "body2", "attributes": {}},
    ]
    response = message_wrapper.send_messages(mock_sqs_queue, messages)
    assert len(response["Successful"]) == 1
    assert response["Successful"][0]["MessageId"] == "abc"


def test_send_messages_failure(mock_sqs_queue):
    """Test sending a batch of messages with failures."""
    mock_sqs_queue.send_messages.return_value = {
        "Successful": [],
        "Failed": [
            {"Id": "0", "MessageId": "xyz", "SenderFault": True, "Code": "Invalid"}
        ],
    }
    messages = [{"body": "body1", "attributes": {}}]
    response = message_wrapper.send_messages(mock_sqs_queue, messages)
    assert not response["Successful"]
    assert len(response["Failed"]) == 1


def test_send_messages_client_error(mock_sqs_queue):
    """Test ClientError on sending a batch of messages."""
    mock_sqs_queue.send_messages.side_effect = ClientError({}, "SendMessages")
    with pytest.raises(ClientError):
        message_wrapper.send_messages(mock_sqs_queue, [])


def test_receive_messages(mock_sqs_queue):
    """Test receiving messages."""
    messages = message_wrapper.receive_messages(mock_sqs_queue, 1, 1)
    mock_sqs_queue.receive_messages.assert_called_once_with(
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )
    assert len(messages) == 1
    assert messages[0].body == "test body"


def test_receive_messages_client_error(mock_sqs_queue):
    """Test ClientError on receiving messages."""
    mock_sqs_queue.receive_messages.side_effect = ClientError({}, "ReceiveMessage")
    with pytest.raises(ClientError):
        message_wrapper.receive_messages(mock_sqs_queue, 1, 1)


def test_delete_message(mock_sqs_queue):
    """Test deleting a single message."""
    mock_message = MagicMock()
    mock_message.message_id = "789"
    message_wrapper.delete_message(mock_message)
    mock_message.delete.assert_called_once()


def test_delete_message_client_error():
    """Test ClientError on deleting a message."""
    mock_message = MagicMock()
    mock_message.message_id = "789"
    mock_message.delete.side_effect = ClientError({}, "DeleteMessage")
    with pytest.raises(ClientError):
        message_wrapper.delete_message(mock_message)


def test_delete_messages(mock_sqs_queue):
    """Test deleting a batch of messages."""
    mock_message = MagicMock()
    mock_message.receipt_handle = "handle_456"
    messages = [mock_message]
    response = message_wrapper.delete_messages(mock_sqs_queue, messages)
    mock_sqs_queue.delete_messages.assert_called_once()
    assert len(response["Successful"]) == 1


def test_delete_messages_failure(mock_sqs_queue):
    """Test deleting a batch of messages with failures."""
    mock_sqs_queue.delete_messages.return_value = {
        "Successful": [],
        "Failed": [{"Id": "0"}],
    }
    mock_message = MagicMock()
    mock_message.receipt_handle = "handle_456"
    messages = [mock_message]
    response = message_wrapper.delete_messages(mock_sqs_queue, messages)
    assert not response["Successful"]
    assert len(response["Failed"]) == 1


def test_delete_messages_client_error(mock_sqs_queue):
    """Test ClientError on deleting a batch of messages."""
    mock_sqs_queue.delete_messages.side_effect = ClientError({}, "DeleteMessages")
    with pytest.raises(ClientError):
        message_wrapper.delete_messages(mock_sqs_queue, [])


def test_parse_message_body_valid():
    """Test parse_message_body with valid JSON and both fields present."""
    message = MagicMock()
    message.body = '{"domain": "example.com", "next": "url2"}'
    domain, next_url = message_wrapper.parse_message_body(message)
    assert domain == "example.com"
    assert next_url == "url2"


def test_parse_message_body_missing_fields():
    """Test parse_message_body with missing fields."""
    message = MagicMock()
    message.body = '{"domain": "example.com"}'
    domain, next_url = message_wrapper.parse_message_body(message)
    assert domain == "example.com"
    assert next_url is None


def test_parse_message_body_invalid_json():
    """Test parse_message_body with invalid JSON."""
    message = MagicMock()
    message.body = "{invalid json}"
    domain, next_url = message_wrapper.parse_message_body(message)
    assert domain is None
    assert next_url is None


def test_parse_message_body_no_body():
    """Test parse_message_body with missing body attribute."""
    message = MagicMock()
    del message.body
    domain, next_url = message_wrapper.parse_message_body(message)
    assert domain is None
    assert next_url is None


@pytest.mark.asyncio
async def test_visibility_heartbeat(monkeypatch):
    """
    Test that visibility_heartbeat periodically calls change_message_visibility and stops on event.
    """
    message = MagicMock()
    message.receipt_handle = "handle_123"
    calls = []

    # Simulate change_message_visibility function
    def fake_change_message_visibility(**kwargs):
        calls.append(kwargs)

    message.change_visibility = fake_change_message_visibility

    monkeypatch.setattr(
        asyncio, "to_thread", lambda func, *args, **kwargs: func(*args, **kwargs)
    )
    stop_event = asyncio.Event()

    task = message_wrapper.visibility_heartbeat(
        message, stop_event, extend_timeout=5, interval=1
    )

    async def stop_soon():
        await asyncio.sleep(2.5)
        stop_event.set()

    await asyncio.gather(task, stop_soon())

    assert len(calls) >= 1, f"change_message_visibility was not called: {calls}"

    for call_kwargs in calls:
        assert call_kwargs["VisibilityTimeout"] == 5
