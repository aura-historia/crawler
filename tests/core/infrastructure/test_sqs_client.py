import logging
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from src.core.infrastructure.sqs_client import SQSClient


@pytest.fixture
def boto_stubs(monkeypatch):
    queue = MagicMock()
    queue.url = "https://sqs.test/default"

    queue_collection = MagicMock()
    queue_collection.filter.return_value = []
    queue_collection.all.return_value = []

    sqs_resource = MagicMock()
    sqs_resource.get_queue_by_name.return_value = queue
    sqs_resource.create_queue.return_value = queue
    sqs_resource.queues = queue_collection

    session = MagicMock()
    session.region_name = "us-east-1"
    session.resource.return_value = sqs_resource

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr(
        "src.core.infrastructure.sqs_client.boto3.Session",
        lambda: session,
    )

    return {"queue": queue, "resource": sqs_resource, "session": session}


@pytest.fixture
def client_and_stubs(monkeypatch):
    queue = MagicMock()
    queue.url = "https://sqs.test/default"

    sqs_resource = MagicMock()
    sqs_resource.get_queue_by_name.return_value = queue
    sqs_resource.create_queue.return_value = queue
    sqs_resource.queues.all.return_value = [queue]
    sqs_resource.queues.filter.return_value = [queue]

    session = MagicMock()
    session.region_name = "us-east-1"
    session.resource.return_value = sqs_resource

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr(
        "src.core.infrastructure.sqs_client.boto3.Session", lambda: session
    )

    return SQSClient("default-queue", {}), queue, sqs_resource


def test_init_caches_default_queue(boto_stubs):
    client = SQSClient("default-queue", {"VisibilityTimeout": "30"})

    assert client.queue is boto_stubs["queue"]
    boto_stubs["resource"].get_queue_by_name.assert_called_once_with(
        QueueName="default-queue"
    )
    boto_stubs["resource"].create_queue.assert_not_called()


def test_init_creates_queue_on_missing_default(boto_stubs):
    boto_stubs["resource"].get_queue_by_name.side_effect = ClientError(
        {"Error": {"Code": "QueueDoesNotExist", "Message": ""}},
        "GetQueueUrl",
    )

    client = SQSClient("default-queue", {"VisibilityTimeout": "45"})

    boto_stubs["resource"].create_queue.assert_called_once_with(
        QueueName="default-queue",
        Attributes={"VisibilityTimeout": "45"},
    )
    assert client.queue is boto_stubs["queue"]


def test_init_re_raises_unexpected_client_error(boto_stubs):
    boto_stubs["resource"].get_queue_by_name.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "nope"}},
        "GetQueueUrl",
    )

    with pytest.raises(ClientError):
        SQSClient("default-queue", {})

    boto_stubs["resource"].create_queue.assert_not_called()


def test_get_active_queue_uses_cache(boto_stubs):
    client = SQSClient("default-queue", {})
    boto_stubs["resource"].get_queue_by_name.reset_mock()

    queue = client._get_active_queue()

    assert queue is boto_stubs["queue"]
    boto_stubs["resource"].get_queue_by_name.assert_not_called()


def test_get_active_queue_fetches_named_queue(boto_stubs):
    other_queue = MagicMock()
    boto_stubs["resource"].get_queue_by_name.return_value = other_queue
    client = SQSClient("default-queue", {})

    queue = client._get_active_queue(name="custom-queue")

    assert queue is other_queue
    boto_stubs["resource"].get_queue_by_name.assert_called_with(
        QueueName="custom-queue"
    )


def test_send_message_delegates_to_queue(boto_stubs):
    client = SQSClient("default-queue", {})
    boto_stubs["queue"].send_message.return_value = {"MessageId": "mid-1"}

    response = client.send_message(
        "hello", {"attr": {"DataType": "String", "StringValue": "x"}}
    )

    assert response == {"MessageId": "mid-1"}
    boto_stubs["queue"].send_message.assert_called_once_with(
        MessageBody="hello",
        MessageAttributes={"attr": {"DataType": "String", "StringValue": "x"}},
    )


def test_send_message_supports_queue_override(boto_stubs):
    client = SQSClient("default-queue", {})
    other_queue = MagicMock()
    other_queue.send_message.return_value = {"MessageId": "m-override"}
    client.get_queue = MagicMock(return_value=other_queue)

    response = client.send_message("hello", name="custom-queue")

    client.get_queue.assert_called_once_with("custom-queue")
    other_queue.send_message.assert_called_once()
    assert response == {"MessageId": "m-override"}


def test_send_messages_batches_entries(boto_stubs):
    client = SQSClient("default-queue", {})
    boto_stubs["queue"].send_messages.return_value = {"Successful": []}
    payload = [
        {"body": "one", "attributes": {"foo": "bar"}},
        {"body": "two"},
    ]

    client.send_messages(payload)

    sent_entries = boto_stubs["queue"].send_messages.call_args.kwargs["Entries"]
    assert sent_entries == [
        {
            "Id": "0",
            "MessageBody": "one",
            "MessageAttributes": {"foo": "bar"},
        },
        {
            "Id": "1",
            "MessageBody": "two",
            "MessageAttributes": {},
        },
    ]


def test_send_messages_supports_queue_override(boto_stubs):
    client = SQSClient("default-queue", {})
    other_queue = MagicMock()
    client.get_queue = MagicMock(return_value=other_queue)
    other_queue.send_messages.return_value = {"Successful": []}

    client.send_messages([{"body": "one"}], name="custom-queue")

    client.get_queue.assert_called_once_with("custom-queue")
    other_queue.send_messages.assert_called_once()


def test_receive_messages_returns_queue_results(boto_stubs):
    client = SQSClient("default-queue", {})
    message_mock = MagicMock()
    message_mock.message_id = "123"
    message_mock.body = "payload"
    boto_stubs["queue"].receive_messages.return_value = [message_mock]

    messages = client.receive_messages(max_number=2, wait_time=1)

    assert messages == [message_mock]
    boto_stubs["queue"].receive_messages.assert_called_once_with(
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=2,
        WaitTimeSeconds=1,
    )


def test_receive_messages_supports_queue_override(boto_stubs):
    client = SQSClient("default-queue", {})
    other_queue = MagicMock()
    other_queue.receive_messages.return_value = []
    client.get_queue = MagicMock(return_value=other_queue)

    client.receive_messages(max_number=1, wait_time=0, name="custom-queue")

    client.get_queue.assert_called_once_with("custom-queue")
    other_queue.receive_messages.assert_called_once_with(
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
    )


def test_delete_messages_uses_default_queue(boto_stubs):
    client = SQSClient("default-queue", {})
    boto_stubs["queue"].delete_messages.return_value = {"Successful": []}
    fake_messages = [MagicMock(receipt_handle="rh-1"), MagicMock(receipt_handle="rh-2")]

    client.delete_messages(fake_messages)

    boto_stubs["queue"].delete_messages.assert_called_once_with(
        Entries=[
            {"Id": "0", "ReceiptHandle": "rh-1"},
            {"Id": "1", "ReceiptHandle": "rh-2"},
        ]
    )


def test_delete_messages_supports_queue_override(boto_stubs):
    client = SQSClient("default-queue", {})
    other_queue = MagicMock()
    other_queue.delete_messages.return_value = {"Successful": []}
    client.get_queue = MagicMock(return_value=other_queue)
    fake_messages = [MagicMock(receipt_handle="rh-1")]

    client.delete_messages(fake_messages, name="custom-queue")

    client.get_queue.assert_called_once_with("custom-queue")
    other_queue.delete_messages.assert_called_once()


def test_create_queue_success(client_and_stubs):
    client, queue, sqs_resource = client_and_stubs
    sqs_resource.create_queue.return_value = queue

    created = client.create_queue({"VisibilityTimeout": "60"}, name="new-queue")

    sqs_resource.create_queue.assert_called_once_with(
        QueueName="new-queue", Attributes={"VisibilityTimeout": "60"}
    )
    assert created is queue


def test_create_queue_failure(client_and_stubs):
    client, _, sqs_resource = client_and_stubs
    sqs_resource.create_queue.side_effect = ClientError(
        {"Error": {"Code": "Throttling", "Message": "boom"}},
        "CreateQueue",
    )

    with pytest.raises(ClientError):
        client.create_queue(name="new-queue")


def test_get_queue_success(client_and_stubs):
    client, queue, sqs_resource = client_and_stubs

    fetched = client.get_queue(name="existing")

    sqs_resource.get_queue_by_name.assert_called_with(QueueName="existing")
    assert fetched is queue


def test_get_queue_failure(client_and_stubs):
    client, _, sqs_resource = client_and_stubs
    sqs_resource.get_queue_by_name.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "boom"}},
        "GetQueueUrl",
    )

    with pytest.raises(ClientError):
        client.get_queue(name="existing")


def test_get_queues_with_prefix(client_and_stubs):
    client, queue, sqs_resource = client_and_stubs

    queues = client.get_queues(prefix="pre")

    sqs_resource.queues.filter.assert_called_once_with(QueueNamePrefix="pre")
    assert queues == [queue]


def test_get_queues_without_prefix(client_and_stubs):
    client, queue, sqs_resource = client_and_stubs

    queues = client.get_queues()

    sqs_resource.queues.all.assert_called_once()
    assert queues == [queue]


def test_remove_queue_clears_cache_on_match(client_and_stubs):
    client, queue, _ = client_and_stubs

    client.remove_queue(queue)

    assert client.queue is None


def test_remove_queue_logs_error_on_failure(client_and_stubs, caplog):
    client, queue, _ = client_and_stubs
    queue.delete.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "boom"}},
        "DeleteQueue",
    )
    caplog.set_level(logging.ERROR)

    with pytest.raises(ClientError):
        client.remove_queue(queue)

    assert "Couldn't delete queue" in caplog.text


def test_send_message_logs_and_reraises_on_failure(boto_stubs, caplog):
    client = SQSClient("default-queue", {})
    boto_stubs["queue"].send_message.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "broken"}},
        "SendMessage",
    )
    caplog.set_level(logging.ERROR)

    with pytest.raises(ClientError):
        client.send_message("hello")

    assert "Send message failed" in caplog.text


def test_send_messages_logs_failures(boto_stubs, caplog):
    client = SQSClient("default-queue", {})
    boto_stubs["queue"].send_messages.return_value = {
        "Successful": [{"Id": "0", "MessageId": "mid-1"}],
        "Failed": [{"Id": "1", "MessageId": "mid-2"}],
    }
    caplog.set_level(logging.WARNING)

    response = client.send_messages(
        [
            {"body": "ok"},
            {"body": "bad"},
        ]
    )

    assert response["Failed"]
    assert "Failed to send" in caplog.text


def test_receive_messages_logs_and_reraises_on_failure(boto_stubs, caplog):
    client = SQSClient("default-queue", {})
    boto_stubs["queue"].receive_messages.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "boom"}},
        "ReceiveMessage",
    )
    caplog.set_level(logging.ERROR)

    with pytest.raises(ClientError):
        client.receive_messages(max_number=1, wait_time=0)

    assert "Couldn't receive messages" in caplog.text


def test_delete_messages_logs_failures_and_reraises(boto_stubs, caplog):
    client = SQSClient("default-queue", {})
    boto_stubs["queue"].delete_messages.return_value = {
        "Successful": [],
        "Failed": [{"Id": "0", "MessageId": "mid-err"}],
    }
    fake_messages = [MagicMock(receipt_handle="rh-1")]
    caplog.set_level(logging.WARNING)

    response = client.delete_messages(fake_messages)

    assert response["Failed"]
    assert "Could not delete" in caplog.text


def test_delete_messages_reraises_client_error(boto_stubs, caplog):
    client = SQSClient("default-queue", {})
    boto_stubs["queue"].delete_messages.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "boom"}},
        "DeleteMessageBatch",
    )
    fake_messages = [MagicMock(receipt_handle="rh-1")]
    caplog.set_level(logging.ERROR)

    with pytest.raises(ClientError):
        client.delete_messages(fake_messages)

    assert "Couldn't delete messages from queue" in caplog.text


def test_delete_message_deletes_successfully(client_and_stubs):
    client, _, _ = client_and_stubs
    message = MagicMock()
    message.message_id = "mid-1"

    client.delete_message(message)

    message.delete.assert_called_once()


def test_delete_message_reraises_client_error(client_and_stubs, caplog):
    client, _, _ = client_and_stubs
    message = MagicMock()
    message.message_id = "mid-err"
    message.delete.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "boom"}},
        "DeleteMessage",
    )
    caplog.set_level(logging.ERROR)

    with pytest.raises(ClientError):
        client.delete_message(message)

    assert "Couldn't delete message" in caplog.text
