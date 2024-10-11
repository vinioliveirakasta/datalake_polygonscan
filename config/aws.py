import boto3
import botocore.exceptions as boto_exception
import logging
import config.logging as logging_config


logging_config.set_logging_config(root_level='INFO')

session = None
secrets_client = None
rs_db_name = 'prod'  # Directly set to 'prod' since no staging environment exists for the Polygonscan API
s3_bucket = None

def set_session():
    """
    Sets the AWS session and initializes the Secrets Manager Client for the production environment.
    If running locally, it will use the 'datalake_prod_operator' profile.
    """
    global session
    global secrets_client
    global s3_bucket

    try:
        # Check if running on Lambda or other AWS service
        if boto3.Session().get_credentials() is not None:
            logging.info("Running prod setup using instance profile credentials")
            session = boto3.Session()
        else:
            # If not running on Lambda, use the local profile
            logging.warning('Running prod setup with local credentials')
            session = boto3.Session(profile_name='datalake_prod_operator')

        # Initialize the Secrets Manager Client for production
        secrets_client = session.client(service_name='secretsmanager', region_name='eu-west-1')

    except boto_exception.ClientError as e:
        logging.error(f'Could not establish session from IAM role... {e}')
        raise e

    if secrets_client is None:
        logging.error('Secrets client could not be initialized.')
        raise ValueError('Secrets client is not initialized.')

    # Set the S3 bucket name for the production environment
    s3_bucket = 'datalake-prod-backend-data-extract'
    logging.warning(f"AWS connection variables: rs_db_name={rs_db_name}, bucket={s3_bucket}")
