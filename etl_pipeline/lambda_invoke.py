import boto3
import json
from prefect import task

@task(name="Invoke Lambda Loader")
def invoke_lambda_loader(function_name: str, payload: dict = None):
    lambda_client = boto3.client('lambda')
    
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',  # synchronous invocation
        Payload=json.dumps(payload or {})
    )
    
    result = json.loads(response['Payload'].read())
    
    if 'FunctionError' in response:
        raise Exception(f"Lambda invocation failed: {result}")
    
    return result
