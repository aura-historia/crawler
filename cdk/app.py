import os

import aws_cdk as cdk
from dotenv import load_dotenv

from cdk.stacks.database import DatabaseStack
from cdk.stacks.orchestration_spider_lambda import SpiderOrchestrationStack
from cdk.stacks.queues import QueueStack
from cdk.stacks.shop_registration_lambda import CrawlerStack

load_dotenv(verbose=True)
app = cdk.App()

env_config = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION"),
)

db_stack = DatabaseStack(app, "DatabaseStack", env=env_config)

queue_stack = QueueStack(app, "QueueStack", env=env_config)

shop_registration_lambda_stack = CrawlerStack(
    app,
    "ShopRegistrationLambdaStack",
    table=db_stack.shop_table,  # This reference creates the dependency
    env=env_config,
)

spider_orchestration_stack = SpiderOrchestrationStack(
    app,
    "SpiderOrchestrationStack",
    table=db_stack.shop_table,
    spider_queue=queue_stack.spider_queue,
    env=env_config,
)

app.synth()
