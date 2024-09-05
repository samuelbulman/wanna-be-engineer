# Standard library imports
import json

# Third party imports
import boto3
from botocore.exceptions import ClientError

# Local imports


###  AWS docs for learning more about configurations or implementations: https://aws.amazon.com/developer/language/python/  ###

def fetch_secret(secret_name, region_name="us-east-1") -> dict:
    """Leverage AWS Secrets Manager to securely pass sensitive information into scripts that require credentials for external authentication. Defaults to secrets stored in the `us-east-1` region."""

    region_name = region_name
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        return json.loads(get_secret_value_response['SecretString'], strict=False)
    
    except ClientError as e:
        ### For a list of exceptions thrown, see https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html  ###
        raise e