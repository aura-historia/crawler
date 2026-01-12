"""CDK Stack for SQS queues used by the crawler system."""

from __future__ import annotations

from aws_cdk import Duration, Stack, aws_sqs as sqs
from constructs import Construct


class QueueStack(Stack):
    """Stack that creates SQS queues for the crawler system."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        """Initialize the Queue Stack.

        Args:
            scope: CDK app scope.
            construct_id: Stack ID.
            **kwargs: Additional stack arguments.
        """
        super().__init__(scope, construct_id, **kwargs)

        self.spider_dlq = sqs.Queue(
            self,
            "SpiderProductDLQ",
            queue_name="spider_product_queue_dlq",
            retention_period=Duration.days(14),
        )

        self.spider_dlq_queue = sqs.DeadLetterQueue(
            max_receive_count=10, queue=self.spider_dlq
        )

        self.spider_queue = sqs.Queue(
            self,
            "SpiderProductQueue",
            queue_name="spider_product_queue",
            visibility_timeout=Duration.minutes(10),
            retention_period=Duration.days(7),
            dead_letter_queue=self.spider_dlq_queue,
        )

        self.scraper_dlq = sqs.Queue(
            self,
            "ProductScraperDLQ",
            queue_name="product_scraper_queue_dlq",
            retention_period=Duration.days(14),
        )

        self.scraper_dlq_queue = sqs.DeadLetterQueue(
            max_receive_count=10, queue=self.scraper_dlq
        )

        # Scraper queue for product scraping tasks
        self.scraper_queue = sqs.Queue(
            self,
            "ProductScraperQueue",
            queue_name="product_scraper_queue",
            visibility_timeout=Duration.minutes(10),
            retention_period=Duration.days(7),
            dead_letter_queue=self.scraper_dlq_queue,
        )
