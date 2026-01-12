from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from src.core.aws.database.operations import db_operations

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Global SQS client for connection reuse across warm Lambda invocations
_GLOBAL_SQS_CLIENT: Optional[Any] = None


def _get_sqs_client() -> Any:
    """Get or create a module-level SQS client for reuse across invocations.

    Returns:
        boto3 SQS client instance.
    """
    global _GLOBAL_SQS_CLIENT
    if _GLOBAL_SQS_CLIENT is None:
        endpoint_url = os.getenv("SQS_ENDPOINT_URL")
        if endpoint_url:
            _GLOBAL_SQS_CLIENT = boto3.client("sqs", endpoint_url=endpoint_url)
        else:
            _GLOBAL_SQS_CLIENT = boto3.client("sqs")
    return _GLOBAL_SQS_CLIENT


def _send_batch_to_sqs(
    sqs_client: Any, queue_url: str, messages: List[Dict[str, Any]]
) -> int:
    """Send a batch of messages to SQS queue.

    SQS batch send supports max 10 messages per request.

    Args:
        sqs_client: boto3 SQS client.
        queue_url: SQS queue URL.
        messages: List of message dicts with 'Id' and 'MessageBody' keys.

    Returns:
        Number of successfully sent messages.

    Raises:
        ClientError: If SQS batch send fails.
    """
    if not messages:
        return 0

    try:
        response = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=messages)

        successful = len(response.get("Successful", []))
        failed = response.get("Failed", [])

        if failed:
            logger.error(f"Failed to send {len(failed)} messages to SQS: {failed}")

        return successful

    except ClientError as e:
        logger.error(f"SQS batch send failed: {e}")
        raise


def _enqueue_shops_to_spider_queue(
    domains: List[str], queue_url: str
) -> Dict[str, Any]:
    """Enqueue shop domains to the spider SQS queue in batches.

    Args:
        domains: List of shop domains to enqueue.
        queue_url: SQS queue URL.

    Returns:
        Dict with 'successful' count and 'failed' list of domains.
    """
    sqs_client = _get_sqs_client()
    total_sent = 0
    failed_domains: List[str] = []

    # Process in batches of 10 (SQS limit)
    for i in range(0, len(domains), 10):
        batch = domains[i : i + 10]

        messages = [
            {
                "Id": str(idx),
                "MessageBody": json.dumps({"domain": domain}),
            }
            for idx, domain in enumerate(batch)
        ]

        try:
            sent = _send_batch_to_sqs(sqs_client, queue_url, messages)
            total_sent += sent

            # Track failed domains if not all were sent
            if sent < len(batch):
                failed_domains.extend(batch[sent:])
        except Exception as e:
            logger.error(f"Failed to send batch to SQS: {e}")
            failed_domains.extend(batch)

    logger.info(f"Enqueued {total_sent}/{len(domains)} domains to spider queue")

    if failed_domains:
        logger.warning(
            f"Failed to enqueue {len(failed_domains)} domains: {failed_domains}"
        )

    return {
        "successful": total_sent,
        "failed": failed_domains,
    }


def handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    """AWS Lambda handler for spider orchestration.

    Args:
        event: EventBridge scheduled event or manual invocation.
        context: Lambda context object.

    Returns:
        Dict with status and summary of enqueued shops.
    """
    logger.info("Starting spider orchestration")

    queue_url = os.getenv("SQS_PRODUCT_SPIDER_QUEUE_URL")
    if not queue_url:
        logger.error("SQS_PRODUCT_SPIDER_QUEUE_URL environment variable not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Queue URL not configured"}),
        }

    # Calculate cutoff date (2 days ago from now)
    cutoff_days = int(os.getenv("ORCHESTRATION_CUTOFF_DAYS", "2"))
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
    cutoff_date_str = cutoff_date.isoformat()

    logger.info(f"Querying shops with last_crawled_end < {cutoff_date_str}")

    try:
        # Get shops that need crawling
        shops = db_operations.get_last_crawled_shops(
            cutoff_date=cutoff_date_str, country="DE"
        )

        if not shops:
            logger.info("No shops found requiring crawling")
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "No shops to enqueue",
                        "shops_count": 0,
                        "cutoff_date": cutoff_date_str,
                    }
                ),
            }

        domains = [shop.domain for shop in shops]
        logger.info(f"Found {len(domains)} shops to enqueue")

        # Enqueue domains to SQS spider queue
        enqueue_result = _enqueue_shops_to_spider_queue(domains, queue_url)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Spider orchestration completed",
                    "shops_found": len(domains),
                    "shops_enqueued": enqueue_result["successful"],
                    "shops_failed": len(enqueue_result["failed"]),
                    "failed_domains": enqueue_result["failed"],
                    "cutoff_date": cutoff_date_str,
                }
            ),
        }

    except Exception as e:
        logger.error(f"Spider orchestration failed: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
