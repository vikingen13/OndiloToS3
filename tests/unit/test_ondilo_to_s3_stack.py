import aws_cdk as core
import aws_cdk.assertions as assertions

from ondilo_to_s3.ondilo_to_s3_stack import OndiloToS3Stack

# example tests. To run these tests, uncomment this file along with the example
# resource in ondilo_to_s3/ondilo_to_s3_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = OndiloToS3Stack(app, "ondilo-to-s3")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
