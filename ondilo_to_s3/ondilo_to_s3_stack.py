from aws_cdk import (
    # Duration,
    Stack,
    CfnOutput,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    aws_lambda as lambda_,
    aws_secretsmanager as sm,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    Size,
    aws_kinesisfirehose_alpha as firehose,
    aws_kinesisfirehose_destinations_alpha as destinations,
    # aws_sqs as sqs,
)
from constructs import Construct
from aws_cdk import aws_iam

class OndiloToS3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #first we check if we need to create a S3 bucket or if it is provided as context
        
        #we check if the S3 bucket name is provided as context
        s3bucketname = self.node.try_get_context("s3bucketname")        
        if not s3bucketname:
            #if not we create the S3 bucket
            myS3Bucket = s3.Bucket(self, "OndiloToS3Bucket",
                                versioned=False,
                                removal_policy=RemovalPolicy.DESTROY)
        else:
            #if it is provided, we get the existing bucket
            myS3Bucket = s3.Bucket.from_bucket_name(self, "OndiloToS3Bucket", s3bucketname)

        #we create the secret in secret manager to store the token
        mySecret = sm.Secret(self, "OndiloToken",
                                description="The token used to connect to Ondilo",
                                secret_name="OndiloToken"
                            )
        
        #create the kinesis firehose delivery stream

        myS3Destination = destinations.S3Bucket(myS3Bucket,
                data_output_prefix="ondilodata/device=!{partitionKeyFromQuery:id}/Year=!{partitionKeyFromQuery:yyyy}/Month=!{partitionKeyFromQuery:mm}/Day=!{partitionKeyFromQuery:dd}/",
                error_output_prefix="ondilodata/Failures/result=!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}/",
                buffering_interval=Duration.minutes(5),
                buffering_size=Size.mebibytes(128)
            )
        myFH = firehose.DeliveryStream(self, "Ondilo Delivery Stream",
           destinations=[myS3Destination]
        )
        
        myFH.node.default_child.add_property_override(
            "ExtendedS3DestinationConfiguration",
            {"DynamicPartitioningConfiguration":{ "Enabled": True, "RetryOptions": { "DurationInSeconds": 300 }} ,
                 "ProcessingConfiguration": {
                    "Enabled": True,
                    "Processors": [
                    {
                    "Type": 'MetadataExtraction',
                    "Parameters": [
                        {
                        "ParameterName": 'MetadataExtractionQuery',
                        "ParameterValue": '{id: .id, yyyy: .year, mm: .month, dd: .day}',
                        },
                        {
                        "ParameterName": 'JsonParsingEngine',
                        "ParameterValue": 'JQ-1.6',
                        },
                  ],
                    },
                    {
                        "Type": 'AppendDelimiterToRecord',
                        "Parameters": [
                        {
                        "ParameterName": 'Delimiter',
                        "ParameterValue": '\\n',
                        }
                        ],
                    },
                    ],
                },
                       
             }
        )


        #then we create the lambda function that will be used to collect and store the data
        myLambdaFunction = lambda_.Function(self, "OndiloToS3LambdaFunction",
                                            runtime=lambda_.Runtime.PYTHON_3_10,
                                            handler="lambda_function.lambda_handler",
                                            code=lambda_.Code.from_asset("ondilo_to_s3/OndiloToS3LambdaFunction"),
                                            environment={
                                                "OndiloTokenSecretName": mySecret.secret_name,
                                                "KinesisStreamName": myFH.delivery_stream_name
                                            },
                                            timeout=Duration.seconds(120)
                                            )
        
        #grant the lambda the right to read the token from secret manager
        mySecret.grant_read(myLambdaFunction)

        #grant the lambda the right to put records
        myFH.grant_put_records(myLambdaFunction)

        #create a daily event bridge rule to trigger the lambda function at 1AM
        myEventBridgeRule = events.Rule(self, "OndiloLambdaDaily",
                                        description="Rule to trigger the lambda function",
                                        schedule=events.Schedule.cron(minute="0", hour="1"),
                                        targets=[targets.LambdaFunction(myLambdaFunction,retry_attempts=2)]
                                        )

        #allow event bridge to invoke the lambda function
        myLambdaFunction.add_permission(
            "Permission_Invoke_Lambda",
            principal=iam.ServicePrincipal('events.amazonaws.com'),
            action="lambda:InvokeFunction",
            source_arn=myEventBridgeRule.rule_arn
        )

        CfnOutput(
            self, "lambdafunctioninvoke",
            value="aws lambda invoke --function-name {} --payload '{{ \"month\": \"true\" }}' --cli-binary-format raw-in-base64-out result.txt".format(myLambdaFunction.function_arn),
            description="Please run this command to collect your last month data",
        )

