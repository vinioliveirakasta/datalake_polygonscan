import logging
import time
from io import StringIO
from contextlib import contextmanager
import psycopg2
import json
from config import aws as aws_config 

# Configure logging
import config.logging as logging_config
logging.config.dictConfig(logging_config.config)


@contextmanager
def connect(dbname):
    """
    Context manager to connect to Redshift and yield a cursor.
    Retrieves credentials from AWS Secrets Manager.
    """
    try:
        # Get Redshift credentials from AWS Secrets Manager
        rs_creds = json.loads(aws_config.secrets_client.get_secret_value(SecretId='redshift/credentials').get('SecretString'))

        # Establish the connection
        connection = psycopg2.connect(
            host=rs_creds.get('host'),
            dbname=dbname,
            user=rs_creds.get('username'),
            password=rs_creds.get('password'),
            port=5439,
        )
        connection.autocommit = True

        # Return the cursor
        with connection.cursor() as cursor:
            yield cursor

    except psycopg2.Error as e:
        logging.error(f"Error connecting to Redshift: {e}")
        raise
    finally:
        if connection:
            connection.close()


class Redshift:
    def __init__(self, schema='public', s3_bucket_name=None):
        """
        Initializes Redshift connection with an existing boto3 session from aws.py.
        Uses the session for creating the S3 client and making Redshift queries.
        """
        self.schema = schema
        self.s3_bucket_name = s3_bucket_name or aws_config.s3_bucket  # Use the bucket from aws_config

        # Validate the session from aws.py
        if aws_config.session is None:
            raise ValueError("Session object in aws.py cannot be None")

        # Initialize the S3 client using the session from aws.py
        self.s3_client = aws_config.session.client('s3')

    def upload_to_s3(self, df, s3_key):
        """
        Uploads a Pandas DataFrame as a CSV to S3.
        """
        logging.info(f"Uploading data to S3 at {s3_key}")

        # Convert DataFrame to CSV format
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload to S3 using the session client
        self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
        logging.info(f"Data successfully uploaded to S3 at {s3_key}")

    def copy_from_s3(self, tablename, s3_key):
        """
        Executes the Redshift COPY command to load data from S3 into the specified Redshift table.
        The IAM role for accessing the S3 bucket is fetched from AWS Secrets Manager.
        """
        # Fetch Redshift credentials, including the IAM role for COPY
        rs_creds = json.loads(aws_config.secrets_client.get_secret_value(SecretId='redshift/credentials').get('SecretString'))
        iam_role_arn = rs_creds.get('service_role')

        copy_sql = f"""
        COPY {self.schema}.{tablename}
        FROM 's3://{self.s3_bucket_name}/{s3_key}'
        IAM_ROLE '{iam_role_arn}'
        FORMAT AS CSV
        IGNOREHEADER 1;
        """

        logging.debug(f"Executing COPY command: {copy_sql}")

        # Connect to Redshift and execute the COPY command
        with connect(aws_config.rs_db_name) as cursor:
            cursor.execute(copy_sql)

        logging.info(f"Data successfully copied from S3 to {self.schema}.{tablename}")

    def load_data_to_redshift(self, df, tablename):
        """
        Uploads the data to S3 and then loads it into Redshift using the COPY command.
        """
        # Generate a unique S3 key based on the current timestamp
        s3_key = f"polygonscan/polygon_transactions_{int(time.time())}.csv"

        # Step 1: Upload the DataFrame to S3
        self.upload_to_s3(df, s3_key)

        # Step 2: Execute the COPY command to load data into Redshift from S3
        self.copy_from_s3(tablename, s3_key)

        logging.info(f"Data successfully loaded into Redshift table {tablename}")
