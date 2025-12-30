import os

import aws_cdk as cdk
from dotenv import load_dotenv

from stacks.shop_registration_lambda import CrawlerStack
from stacks.database import DatabaseStack

load_dotenv(verbose=True)
app = cdk.App()

env_config = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION"),
)

db_stack = DatabaseStack(app, "DatabaseStack", env=env_config)

shop_registration_lambda_stack = CrawlerStack(
    app,
    "ShopRegistrationLambdaStack",
    table=db_stack.shop_table,  # This reference creates the dependency
    env=env_config,
)

app.synth()
