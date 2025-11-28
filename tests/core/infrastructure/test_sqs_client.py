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
