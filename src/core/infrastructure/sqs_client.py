import logging
import os
from typing import Optional, Dict, Any, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class SQSClient:
    """Lightweight wrapper around boto3 SQS resource for this project.

    The instance stores a default queue name (self.name) and a cached
    SQS Queue object (self.queue). Each method accepts an optional name argument
    to override the default queue name. When no name is provided the instance
    default is used; if neither is available a ValueError is raised.
    """

    def __init__(self, name: str, attributes: Dict[str, str]):
        session = boto3.Session()
        region_name = os.getenv("AWS_REGION") or session.region_name
        if not region_name:
            raise ValueError(
                "AWS region must be configured via AWS_REGION or the boto3 default profile"
            )
        if not name:
            raise ValueError("SQSClient requires a default queue name")

        self.sqs = session.resource("sqs", region_name=region_name)
        self.name = name
        self.queue = None
        queue_attributes = attributes or {}

        try:
            self.queue = self.sqs.get_queue_by_name(QueueName=self.name)
            logger.info(
                "Cached default queue '%s' with URL=%s", self.name, self.queue.url
            )
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code", "")
            if error_code in {
                "QueueDoesNotExist",
                "AWS.SimpleQueueService.NonExistentQueue",
            }:
                logger.info(
                    "Default queue '%s' not found during init; attempting to create it",
                    self.name,
                )
                self.queue = self.sqs.create_queue(
                    QueueName=self.name,
                    Attributes=queue_attributes,
                )
                logger.info(
                    "Cached default queue '%s' with URL=%s", self.name, self.queue.url
                )
            else:
                logger.exception(
                    "Failed to cache default queue '%s' due to unexpected error",
                    self.name,
                )
                raise

    def create_queue(
        self, attributes: Optional[Dict[str, str]] = None, name: Optional[str] = None
    ):
        """
        Creates an Amazon SQS queue.

        :param attributes: The attributes of the queue, such as maximum message size or
                           whether it's a FIFO queue.
        :param name: Optional queue name to override the instance default.
        :return: A Queue object that contains metadata about the queue and that can be used
                 to perform queue operations like sending and receiving messages.
        """
        if attributes is None:
            attributes = {}

        queue_name = name or self.name

        if not queue_name:
            raise ValueError(
                "Queue name must be provided either as an argument or as the client's default name"
            )

        try:
            queue = self.sqs.create_queue(QueueName=queue_name, Attributes=attributes)
            logger.info("Created queue '%s' with URL=%s", queue_name, queue.url)
            # If this was the client's default name, cache the queue object
            if queue_name == self.name:
                self.queue = queue
        except ClientError as error:
            logger.exception("Couldn't create queue named '%s'.", queue_name)
            raise error
        else:
            return queue

    def get_queue(self, name: Optional[str] = None):
        """
        Gets an SQS queue by name.

        :param name: The name that was used to create the queue. If omitted the
                     instance default name is used.
        :return: A Queue object.
        """
        queue_name = name or self.name

        if not queue_name:
            raise ValueError(
                "Queue name must be provided either as an argument or as the client's default name"
            )
        try:
            queue = self.sqs.get_queue_by_name(QueueName=queue_name)
            logger.info("Got queue '%s' with URL=%s", queue_name, queue.url)
            # cache default queue if applicable
            if queue_name == self.name:
                self.queue = queue
        except ClientError as error:
            logger.exception("Couldn't get queue named %s.", queue_name)
            raise error
        else:
            return queue

    def _get_active_queue(self, name: Optional[str] = None):
        """Return a cached queue if possible, otherwise fetch it by name."""
        queue_name = name or self.name
        if not queue_name:
            raise ValueError(
                "Queue name must be provided either per call or via the default"
            )
        if not name and self.queue is not None:
            return self.queue
        return self.get_queue(queue_name)

    def get_queues(self, prefix: Optional[str] = None) -> List[Any]:
        """
        Gets a list of SQS queues. When a prefix is specified, only queues with names
        that start with the prefix are returned.

        :param prefix: The prefix used to restrict the list of returned queues.
        :return: A list of Queue objects.
        """
        if prefix:
            queue_iter = self.sqs.queues.filter(QueueNamePrefix=prefix)
        else:
            queue_iter = self.sqs.queues.all()
        queues = list(queue_iter)
        if queues:
            logger.info("Got queues: %s", ", ".join([q.url for q in queues]))
        else:
            logger.warning("No queues found.")
        return queues

    def remove_queue(self, queue):
        """
        Removes an SQS queue. When run against an AWS account, it can take up to
        60 seconds before the queue is actually deleted.

        :param queue: The queue to delete.
        :return: None
        """
        try:
            queue.delete()
            logger.info("Deleted queue with URL=%s.", queue.url)
            # if this was the cached default queue, clear it
            if getattr(self.queue, "url", None) == getattr(queue, "url", None):
                self.queue = None
        except ClientError as error:
            logger.exception(
                "Couldn't delete queue with URL=%s!", getattr(queue, "url", "<unknown>")
            )
            raise error

    def send_message(self, message_body, message_attributes=None):
        """
        Send a message to an Amazon SQS queue.

        :param message_body: The body text of the message.
        :param message_attributes: Custom attributes of the message. These are key-value
                                   pairs that can be whatever you want.
        :return: The response from SQS that contains the assigned message ID.
        """
        if not message_attributes:
            message_attributes = {}

        queue = self._get_active_queue()
        try:
            response = queue.send_message(
                MessageBody=message_body, MessageAttributes=message_attributes
            )
        except ClientError as error:
            logger.exception("Send message failed: %s", message_body)
            raise error
        else:
            return response

    def send_messages(self, messages):
        """
        Send a batch of messages in a single request to an SQS queue.
        This request may return overall success even when some messages were not sent.
        The caller must inspect the Successful and Failed lists in the response and
        resend any failed messages.

        :param messages: The messages to send to the queue. These are simplified to
                         contain only the message body and attributes.
        :return: The response from SQS that contains the list of successful and failed
                 messages.
        """
        try:
            entries = [
                {
                    "Id": str(ind),
                    "MessageBody": msg["body"],
                    "MessageAttributes": msg.get("attributes") or {},
                }
                for ind, msg in enumerate(messages)
            ]
            queue = self._get_active_queue()
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
            logger.exception("Send messages failed to queue: %s", self.queue)
            raise error
        else:
            return response

    def receive_messages(self, max_number, wait_time):
        """
        Receive a batch of messages in a single request from an SQS queue.

        :param max_number: The maximum number of messages to receive. The actual number
                           of messages received might be less.
        :param wait_time: The maximum time to wait (in seconds) before returning. When
                          this number is greater than zero, long polling is used. This
                          can result in reduced costs and fewer false empty responses.
        :return: The list of Message objects received. These each contain the body
                 of the message and metadata and custom attributes.
        """
        try:
            messages = self._get_active_queue().receive_messages(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=max_number,
                WaitTimeSeconds=wait_time,
            )
            for msg in messages:
                logger.info("Received message: %s: %s", msg.message_id, msg.body)
        except ClientError as error:
            logger.exception("Couldn't receive messages from queue: %s", self.queue)
            raise error
        else:
            return messages

    def delete_message(self, message):
        """
        Delete a message from a queue. Clients must delete messages after they
        are received and processed to remove them from the queue.

        :param message: The message to delete. The message's queue URL is contained in
                        the message's metadata.
        :return: None
        """
        try:
            message.delete()
            logger.info("Deleted message: %s", message.message_id)
        except ClientError as error:
            logger.exception("Couldn't delete message: %s", message.message_id)
            raise error

    def delete_messages(self, messages):
        """
        Delete a batch of messages from the default queue in a single request.

        :param messages: The list of messages to delete.
        :return: The response from SQS that contains the list of successful and failed
                 message deletions.
        """

        queue = None

        try:
            entries = [
                {"Id": str(ind), "ReceiptHandle": msg.receipt_handle}
                for ind, msg in enumerate(messages)
            ]
            queue = self._get_active_queue()
            response = queue.delete_messages(Entries=entries)
            if "Successful" in response:
                for msg_meta in response["Successful"]:
                    logger.info(
                        "Deleted %s", messages[int(msg_meta["Id"])].receipt_handle
                    )
            if "Failed" in response:
                for msg_meta in response["Failed"]:
                    logger.warning(
                        "Could not delete %s",
                        messages[int(msg_meta["Id"])].receipt_handle,
                    )
        except ClientError:
            logger.exception(
                "Couldn't delete messages from queue %s",
                getattr(queue, "url", "<unknown>"),
            )
        else:
            return response
