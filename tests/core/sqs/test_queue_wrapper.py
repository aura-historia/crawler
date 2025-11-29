import pytest
from unittest.mock import MagicMock, patch
from src.core.sqs import queue_wrapper
from botocore.exceptions import ClientError


@pytest.fixture
def mock_sqs_resource():
    """Fixture for a mocked SQS resource."""
    with patch("src.core.sqs.queue_wrapper.sqs") as mock_sqs:
        # Mock for create_queue
        mock_queue = MagicMock()
        mock_queue.url = "http://queue.url"
        mock_sqs.create_queue.return_value = mock_queue

        # Mock for get_queue_by_name
        mock_sqs.get_queue_by_name.return_value = mock_queue

        # Mock for queues.all and queues.filter
        mock_sqs.queues.all.return_value = [mock_queue]
        mock_sqs.queues.filter.return_value = [mock_queue]

        yield mock_sqs


def test_create_queue(mock_sqs_resource):
    """Test creating a queue."""
    queue = queue_wrapper.create_queue("test-queue")
    mock_sqs_resource.create_queue.assert_called_once_with(
        QueueName="test-queue", Attributes={}
    )
    assert queue.url == "http://queue.url"


def test_create_queue_with_attributes(mock_sqs_resource):
    """Test creating a queue with attributes."""
    attributes = {"DelaySeconds": "120"}
    queue_wrapper.create_queue("test-queue", attributes)
    mock_sqs_resource.create_queue.assert_called_once_with(
        QueueName="test-queue", Attributes=attributes
    )


def test_create_queue_client_error(mock_sqs_resource):
    """Test ClientError on creating a queue."""
    mock_sqs_resource.create_queue.side_effect = ClientError({}, "CreateQueue")
    with pytest.raises(ClientError):
        queue_wrapper.create_queue("test-queue")


def test_get_queue(mock_sqs_resource):
    """Test getting a queue by name."""
    queue = queue_wrapper.get_queue("test-queue")
    mock_sqs_resource.get_queue_by_name.assert_called_once_with(QueueName="test-queue")
    assert queue.url == "http://queue.url"


def test_get_queue_client_error(mock_sqs_resource):
    """Test ClientError on getting a queue."""
    mock_sqs_resource.get_queue_by_name.side_effect = ClientError({}, "GetQueueUrl")
    with pytest.raises(ClientError):
        queue_wrapper.get_queue("test-queue")


def test_get_queues_no_prefix(mock_sqs_resource):
    """Test getting all queues."""
    queues = queue_wrapper.get_queues()
    mock_sqs_resource.queues.all.assert_called_once()
    assert len(queues) == 1
    assert queues[0].url == "http://queue.url"


def test_get_queues_with_prefix(mock_sqs_resource):
    """Test getting queues with a prefix."""
    queues = queue_wrapper.get_queues(prefix="test")
    mock_sqs_resource.queues.filter.assert_called_once_with(QueueNamePrefix="test")
    assert len(queues) == 1


def test_get_queues_none_found(mock_sqs_resource):
    """Test getting queues when none are found."""
    mock_sqs_resource.queues.all.return_value = []
    queues = queue_wrapper.get_queues()
    assert not queues


def test_remove_queue():
    """Test removing a queue."""
    mock_queue = MagicMock()
    mock_queue.url = "http://queue.url"
    queue_wrapper.remove_queue(mock_queue)
    mock_queue.delete.assert_called_once()


def test_remove_queue_client_error():
    """Test ClientError on removing a queue."""
    mock_queue = MagicMock()
    mock_queue.url = "http://queue.url"
    mock_queue.delete.side_effect = ClientError({}, "DeleteQueue")
    with pytest.raises(ClientError):
        queue_wrapper.remove_queue(mock_queue)
