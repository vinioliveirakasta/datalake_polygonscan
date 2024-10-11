# import requests
# import time
# import pandas as pd
# import logging
# from config import polygonscan as polygonscan_config
# from config.aws import set_session 
# from resources.redshift import Redshift
# from requests.exceptions import RequestException

# # Wallet addresses to process
# WALLET_ADDRESSES = [
#         '0x7695ea9a311bca3e49b19807e25a88a7d7505cf6',
#         '0x05247f858e4bbebc4412294acb5afcaa00524a1b',
#         '0x0501FB0476873785aeEd434903f8eB27f6433698',
#         '0xc82257b574e576052b42884953a861fcc5b80f29',
#         '0xac08c80cb31c1879faefd19050bf93f0a3bf3755', 
#         '0x5e1551f6d9dd3a25b09ef2af2532516f83fc7a82',
#         '0x68e0669ea4e10dd08d2e79b15e2d0f01b8c608c5',
#         '0xeeb6fd17507de21c9d3e376c5aaa270990c332bd',
#         '0x5b5e5037c1d4d6695d264b6d6f0de29b2b3726d3',
#         '0x1ec0aC2250261d0E2ee28F63E8253E8D2322383C',
#         '0x885880a80f18c2e5739d110b7f4a0a788ff73d34',
#         '0x8a79526f7a59a56cd5a4166731e54f7e06c9d223',
#         '0xaa05c398958a22efcd88790f25164255d56b11ed',
#         '0x3c00B2c1DC76B33A7BfFFFDd6AAACDd0055e9685',
#         '0x629E717C10Be98307259fD9eE13f26B20cD98b57',
#         '0xf089b4c6c57Ff9ad6c5E6f48b64Cf5e316Ab3aa3',
#         '0x6fCc0Ed62478fDb19b5F1f26f08d0e377Ca1681A',
#         '0xBf65adf16eC4ceE19E149561eD37Fb62cB800471',
#         '0x243E5DA8aD52bEfd23A8E7A438e13BbEDFC9fd84',
#         '0x2470c7B3f86521747FF351F7b570ff098b901088',
#         '0x381013772192BaFF4Ee886AaabD4e80764b6058e',
#         '0x5F08f5843E86eb4E69341427f1d53F3d30d1f527',
#         '0x74f25c18577C73A71dABBd1EC6fdA8810a47DDC6',
#         '0x9F3060cA34A748c6cB265B0250251e58e40DF9BC',
#         '0xe8A4e027E746031DEd5A60572f614f8E9b0ca9Eb',
#         '0xc5b34400df3b99277ff39a8d25a2b6e21b5cc980',
#         '0xb11f1f578ae23f5b9c0bede271a1f73b1e38f3ec',
#         '0x5564509044abae6503599e52496b859b9dfd41cd',
#         '0xb8a8c2f377f82cd041cd97b45c1a1dc2db7238ed'

# ]

# # Define the Redshift schema and table name
# REDSHIFT_SCHEMA = 'polygonscan'
# REDSHIFT_TABLE = 'wallet_txs'

# # Set the logging level (if not already set)
# logging.basicConfig(level=logging.DEBUG)


# def fetch_polygon_transactions(wallet_address, api_key, startblock=0, endblock=99999999, page=1, offset=10000, sort='asc'):
#     """
#     Fetches transaction data from PolygonScan API for a specific wallet address.
#     Handles pagination by using the `page` and `offset` parameters.
#     """
#     transactions = []
#     url = f"{polygonscan_config.DEFAULT_HOST}/api?module=account&action=txlist&address={wallet_address}&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}&sort={sort}&apikey={api_key}"

#     try:
#         while True:
#             response = requests.get(url)
#             response.raise_for_status()
#             data = response.json()

#             if data['status'] == '1':
#                 transactions.extend(data['result'])
#                 if len(data['result']) < offset:
#                     break  # No more transactions to fetch
#                 page += 1  # Move to the next page
#             else:
#                 logging.info(f"No transactions found for wallet {wallet_address} or an error occurred.")
#                 break

#     except RequestException as e:
#         logging.error(f"Error fetching transactions for wallet {wallet_address}: {e}")
#         return []

#     return transactions


# def process_transactions(transactions, wallet_address):
#     """
#     Processes the transaction data to extract relevant fields and format them.
#     """
#     processed_data = []
#     for tx in transactions:
#         processed_data.append({
#             'wallet_address': wallet_address,
#             'tx_hash': tx['hash'],
#             'tx_time': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(tx['timeStamp']))),
#             'from_address': tx['from'],
#             'to_address': tx['to'],
#             'currency': 'Kasta',  
#             'quantity': int(tx['value']) / (10 ** 18),  # Convert from wei to ether units
#             'tx_fee_quantity': int(tx['gasUsed']) * int(tx['gasPrice']) / (10 ** 18),
#             'fee_currency': 'MATIC',  
#         })
#     return processed_data


# def main():
#     """
#     Main function to handle fetching, processing, and inserting transaction data into Redshift via S3 and COPY command.
#     """
#     # Initialize the AWS session for connecting to Redshift and Secrets Manager
#     set_session()
#     logging.info("AWS session initialized successfully.")

#     all_transactions = []

#     # Set API key from AWS Secrets Manager
#     polygonscan_config.set_all()
#     logging.info("PolygonScan API key set.")

#     # Initialize the Redshift class for handling database operations
#     redshift = Redshift(schema=REDSHIFT_SCHEMA)

#     # Iterate over each wallet address
#     for wallet_address in WALLET_ADDRESSES:
#         logging.info(f"Fetching transactions for wallet: {wallet_address}")
#         transactions = fetch_polygon_transactions(wallet_address, polygonscan_config.API_KEY)

#         if transactions:
#             # Process the transactions
#             processed_data = process_transactions(transactions, wallet_address)
#             all_transactions.extend(processed_data)
#         else:
#             logging.info(f"No transactions to display for wallet: {wallet_address}")

#     # Convert all data to a DataFrame
#     if all_transactions:
#         df = pd.DataFrame(all_transactions)
#         logging.info(f"Preparing to upload {len(all_transactions)} transactions to S3 and then to Redshift table {REDSHIFT_TABLE}.")

#         # Upload and load data into Redshift
#         redshift.load_data_to_redshift(df, REDSHIFT_TABLE)

#         logging.info("Transactions have been successfully inserted into Redshift.")
#     else:
#         logging.info("No transactions to display.")


# if __name__ == "__main__":
#     main()

import requests
import time
import pandas as pd
import logging
from config import polygonscan as polygonscan_config
from config.aws import set_session 
from resources.redshift import Redshift
from requests.exceptions import RequestException

# Wallet addresses to process
WALLET_ADDRESSES = [
    '0x7695ea9a311bca3e49b19807e25a88a7d7505cf6',
    '0x05247f858e4bbebc4412294acb5afcaa00524a1b',
    '0x0501FB0476873785aeEd434903f8eB27f6433698',
    '0xc82257b574e576052b42884953a861fcc5b80f29',
    '0xac08c80cb31c1879faefd19050bf93f0a3bf3755', 
    '0x5e1551f6d9dd3a25b09ef2af2532516f83fc7a82',
    '0x68e0669ea4e10dd08d2e79b15e2d0f01b8c608c5',
    '0xeeb6fd17507de21c9d3e376c5aaa270990c332bd',
    '0x5b5e5037c1d4d6695d264b6d6f0de29b2b3726d3',
    '0x1ec0aC2250261d0E2ee28F63E8253E8D2322383C',
    '0x885880a80f18c2e5739d110b7f4a0a788ff73d34',
    '0x8a79526f7a59a56cd5a4166731e54f7e06c9d223',
    '0xaa05c398958a22efcd88790f25164255d56b11ed',
    '0x3c00B2c1DC76B33A7BfFFFDd6AAACDd0055e9685',
    '0x629E717C10Be98307259fD9eE13f26B20cD98b57',
    '0xf089b4c6c57Ff9ad6c5E6f48b64Cf5e316Ab3aa3',
    '0x6fCc0Ed62478fDb19b5F1f26f08d0e377Ca1681A',
    '0xBf65adf16eC4ceE19E149561eD37Fb62cB800471',
    '0x243E5DA8aD52bEfd23A8E7A438e13BbEDFC9fd84',
    '0x2470c7B3f86521747FF351F7b570ff098b901088',
    '0x381013772192BaFF4Ee886AaabD4e80764b6058e',
    '0x5F08f5843E86eb4E69341427f1d53F3d30d1f527',
    '0x74f25c18577C73A71dABBd1EC6fdA8810a47DDC6',
    '0x9F3060cA34A748c6cB265B0250251e58e40DF9BC',
    '0xe8A4e027E746031DEd5A60572f614f8E9b0ca9Eb',
    '0xc5b34400df3b99277ff39a8d25a2b6e21b5cc980',
    '0xb11f1f578ae23f5b9c0bede271a1f73b1e38f3ec',
    '0x5564509044abae6503599e52496b859b9dfd41cd',
    '0xb8a8c2f377f82cd041cd97b45c1a1dc2db7238ed'
]

# Define the Redshift schema and table name
REDSHIFT_SCHEMA = 'polygonscan'
REDSHIFT_TABLE = 'wallet_txs'

# Set the logging level (if not already set)
logging.basicConfig(level=logging.DEBUG)


# def fetch_polygon_transactions(wallet_address, api_key, startblock=0, endblock=99999999, page=1, offset=10000, sort='asc'):
#     """
#     Fetches transaction data from PolygonScan API for a specific wallet address.
#     Handles pagination by using the `page` and `offset` parameters.
#     """
#     transactions = []
#     url = f"{polygonscan_config.DEFAULT_HOST}/api?module=account&action=txlist&address={wallet_address}&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}&sort={sort}&apikey={api_key}"

#     try:
#         while True:
#             response = requests.get(url)
#             response.raise_for_status()
#             data = response.json()

#             if data['status'] == '1':
#                 transactions.extend(data['result'])
#                 if len(data['result']) < offset:
#                     break  # No more transactions to fetch
#                 page += 1  # Move to the next page
#             else:
#                 logging.info(f"No transactions found for wallet {wallet_address} or an error occurred.")
#                 break

#     except RequestException as e:
#         logging.error(f"Error fetching transactions for wallet {wallet_address}: {e}")
#         return []

#     return transactions

def fetch_polygon_transactions(wallet_address, api_key, startblock=0, endblock=99999999, page=1, offset=10000, sort='asc'):
    """
    Fetches transaction data from PolygonScan API for a specific wallet address.
    Handles pagination by using the `page` and `offset` parameters.
    """
    transactions = []
    url = f"{polygonscan_config.DEFAULT_HOST}/api?module=account&action=txlist&address={wallet_address}&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}&sort={sort}&apikey={api_key}"

    try:
        total_fetched = 0  # To keep track of total transactions fetched
        while True:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if data['status'] == '1':
                transactions.extend(data['result'])
                total_fetched += len(data['result'])
                logging.info(f"Fetched {len(data['result'])} transactions for wallet: {wallet_address}, Page: {page}, Total so far: {total_fetched}")

                if len(data['result']) < offset:
                    break  # No more transactions to fetch
                page += 1  # Move to the next page
            else:
                logging.info(f"No transactions found for wallet {wallet_address} or an error occurred.")
                break

    except RequestException as e:
        logging.error(f"Error fetching transactions for wallet {wallet_address}: {e}")
        return []

    logging.info(f"Total transactions fetched for wallet {wallet_address}: {total_fetched}")
    return transactions


def process_transactions(transactions, wallet_address):
    """
    Processes the transaction data to extract relevant fields and format them.
    """
    processed_data = []
    for tx in transactions:
        processed_data.append({
            'wallet_address': wallet_address,
            'tx_hash': tx['hash'],
            'tx_time': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(tx['timeStamp']))),
            'from_address': tx['from'],
            'to_address': tx['to'],
            'currency': 'Kasta',  
            'quantity': int(tx['value']) / (10 ** 18),  # Convert from wei to ether units
            'tx_fee_quantity': int(tx['gasUsed']) * int(tx['gasPrice']) / (10 ** 18),
            'fee_currency': 'MATIC',  
        })
    return processed_data


def main():
    """
    Main function to handle fetching, processing, and inserting transaction data into Redshift via S3 and COPY command.
    """
    # Initialize the AWS session for connecting to Redshift and Secrets Manager
    set_session()
    logging.info("AWS session initialized successfully.")

    all_transactions = []

    # Set API key from AWS Secrets Manager
    polygonscan_config.set_all()
    logging.info("PolygonScan API key set.")

    # Initialize the Redshift class for handling database operations
    redshift = Redshift(schema=REDSHIFT_SCHEMA)

    # Iterate over each wallet address
    for wallet_address in WALLET_ADDRESSES:
        logging.info(f"Fetching transactions for wallet: {wallet_address}")
        transactions = fetch_polygon_transactions(wallet_address, polygonscan_config.API_KEY)

        if transactions:
            # Process the transactions
            processed_data = process_transactions(transactions, wallet_address)
            all_transactions.extend(processed_data)
        else:
            logging.info(f"No transactions to display for wallet: {wallet_address}")

    # Convert all data to a DataFrame
    if all_transactions:
        df = pd.DataFrame(all_transactions)
        logging.info(f"Preparing to upload {len(all_transactions)} transactions to S3 and then to Redshift table {REDSHIFT_TABLE}.")

        # Upload and load data into Redshift
        redshift.load_data_to_redshift(df, REDSHIFT_TABLE)

        logging.info("Transactions have been successfully inserted into Redshift.")
    else:
        logging.info("No transactions to display.")


def handler(event, context):
    """
    Lambda handler function. Calls the main logic.
    """
    main()


# If running locally, call main
if __name__ == "__main__":
    main()
