import json
import config.aws as aws_config
import config.logging as logging_config
import logging.config
logging.config.dictConfig(logging_config.config)

DEFAULT_HOST = 'https://api.polygonscan.com'
DEFAULT_URI = f"{DEFAULT_HOST}/api"

API_KEY = None
HEADERS = None

secret_name = 'prod-datalake-polygonscan-apiKey'

def get_api_key():
    if aws_config.secrets_client is None:
        raise ValueError("Secrets client is not initialized")
    return json.loads(aws_config.secrets_client.get_secret_value(SecretId=secret_name).get('SecretString')).get('apiKey')

def set_api_key():
    global API_KEY
    logging.info(f'Fetching {secret_name}')
    if aws_config.secrets_client is None:
        raise ValueError("Secrets client is not initialized")
    API_KEY = json.loads(aws_config.secrets_client.get_secret_value(SecretId=secret_name).get('SecretString')).get('apiKey')


def set_all():
    set_api_key()

# Ensure to call set_session before any operations
# aws_config.set_session('staging') or aws_config.set_session('prod')
