from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from pythonjsonlogger import json as pythonjson

from src.core.aws.database.operations import db_operations

# Configure JSON logging for CloudWatch
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = pythonjson.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"levelname": "level", "asctime": "timestamp"},
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger.setLevel(log_level)

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
        logger.info(
            "Sending batch to SQS",
            extra={"batch_size": len(messages), "queue_url": queue_url},
        )

        response = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=messages)

        successful = len(response.get("Successful", []))
        failed = response.get("Failed", [])

        if failed:
            logger.error(
                "Failed to send messages to SQS",
                extra={
                    "failed_count": len(failed),
                    "failures": failed,
                    "successful_count": successful,
                },
            )

        logger.info(
            "SQS batch send completed",
            extra={"successful": successful, "failed": len(failed)},
        )

        return successful

    except ClientError as e:
        logger.error(
            "SQS batch send failed",
            extra={
                "error": str(e),
                "error_code": e.response.get("Error", {}).get("Code"),
                "queue_url": queue_url,
            },
        )
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
    batch_count = (len(domains) + 9) // 10  # Round up division
    for i in range(0, len(domains), 10):
        batch_num = (i // 10) + 1
        batch = domains[i : i + 10]

        logger.info(
            f"Processing batch {batch_num}/{batch_count}",
            extra={"batch_domains": batch, "batch_size": len(batch)},
        )

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
                batch_failed = batch[sent:]
                failed_domains.extend(batch_failed)
                logger.warning(
                    f"Batch {batch_num}: partial failure",
                    extra={
                        "sent": sent,
                        "failed": len(batch_failed),
                        "failed_domains": batch_failed,
                    },
                )
        except Exception as e:
            logger.error(
                f"Batch {batch_num}: complete failure",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "failed_domains": batch,
                },
            )
            failed_domains.extend(batch)

    logger.info(
        "Finished enqueueing domains",
        extra={
            "total_domains": len(domains),
            "successful": total_sent,
            "failed": len(failed_domains),
            "success_rate": f"{(total_sent / len(domains) * 100):.1f}%"
            if domains
            else "N/A",
        },
    )

    if failed_domains:
        logger.warning(
            "Some domains failed to enqueue",
            extra={
                "failed_count": len(failed_domains),
                "failed_domains": failed_domains,
            },
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
    logger.info(
        "Spider orchestration lambda invoked",
        extra={"event_source": event.get("source", "manual"), "event": event},
    )

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

    logger.info(
        "Querying shops needing crawl",
        extra={
            "cutoff_date": cutoff_date_str,
            "cutoff_days": cutoff_days,
            "country": "DE",
        },
    )

    try:
        # Get shops that need crawling
        shops = db_operations.get_last_crawled_shops(
            cutoff_date=cutoff_date_str, country="DE"
        )

        if not shops:
            logger.info(
                "No shops found requiring crawling",
                extra={"cutoff_date": cutoff_date_str, "country": "DE"},
            )
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
        logger.info(
            "Found shops requiring crawl",
            extra={
                "shops_count": len(domains),
                "domains_preview": domains[:5],
                "cutoff_date": cutoff_date_str,
            },
        )

        # Enqueue domains to SQS spider queue
        enqueue_result = _enqueue_shops_to_spider_queue(domains, queue_url)

        response_body = {
            "summary": "Spider orchestration completed",
            "shops_found": len(domains),
            "shops_enqueued": enqueue_result["successful"],
            "shops_failed": len(enqueue_result["failed"]),
            "failed_domains": enqueue_result["failed"],
            "cutoff_date": cutoff_date_str,
        }

        logger.info(
            "Spider orchestration completed successfully",
            extra=response_body,
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Spider orchestration completed",
                    "shops_found": response_body["shops_found"],
                    "shops_enqueued": response_body["shops_enqueued"],
                    "shops_failed": response_body["shops_failed"],
                    "failed_domains": response_body["failed_domains"],
                    "cutoff_date": response_body["cutoff_date"],
                }
            ),
        }

    except Exception as e:
        logger.error(
            "Spider orchestration failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "cutoff_date": cutoff_date_str,
            },
            exc_info=True,
        )
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
