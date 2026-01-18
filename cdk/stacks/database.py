from aws_cdk import Stack, aws_dynamodb as dynamodb
from constructs import Construct


class DatabaseStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.shop_table = dynamodb.Table(
            self,
            "ShopMetadataTable",
            table_name="aura-historia-data",
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
        )

        # GSI1 - ProductTypeIndex
        # Queries all URLs of a specific type (e.g., products) within a shop
        self.shop_table.add_global_secondary_index(
            index_name="GSI1",
            partition_key=dynamodb.Attribute(
                name="gsi1_pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="gsi1_sk", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        # GSI2 - CountryLastCrawledIndex
        # Queries shops by country and last_crawled_end timestamp
        # Includes shops that were never crawled (using sentinel timestamp)
        self.shop_table.add_global_secondary_index(
            index_name="GSI2",
            partition_key=dynamodb.Attribute(
                name="gsi2_pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="gsi2_sk", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=["domain"],
        )
        # GSI3 - CountryLastScrapedIndex
        # Queries shops by country and last_scraped_end timestamp
        # Includes shops that were never scraped (using sentinel timestamp)
        self.shop_table.add_global_secondary_index(
            index_name="GSI3",
            partition_key=dynamodb.Attribute(
                name="gsi3_pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="gsi3_sk", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=["domain"],
        )
        # GSI4 - CoreDomainNameIndex
        # Associates different domains that share the same core domain name
        self.shop_table.add_global_secondary_index(
            index_name="GSI4",
            partition_key=dynamodb.Attribute(
                name="gsi4_pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="gsi4_sk", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
