import os
import json
import sys
import logging
from dotenv import load_dotenv
import certifi
import pymongo
import numpy as np
import pandas as pd
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# Configure basic logging if not already set in networksecurity.logging.logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('network_security.log'),  # Log to a file
        logging.StreamHandler()  # Also log to console
    ]
)

# Load environment variables
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
ca = certifi.where()

# Log the MongoDB URL (masking the password for security)
logging.info(f"Loaded MongoDB URL: {MONGO_DB_URL.split('@')[0]}@... (password masked)")

class NetworkSecurityData:
    def __init__(self, data):
        try:
            logging.info("Initializing NetworkSecurityData")
            self.data = data
        except Exception as e:
            logging.error(f"Error initializing NetworkSecurityData: {str(e)}")
            raise NetworkSecurityException(e, sys)

    def csv_to_json_converter(self, file_path):
        try:
            logging.info(f"Reading CSV file: {file_path}")
            # Read the CSV file
            data = pd.read_csv(file_path)
            logging.info(f"Successfully read CSV with {len(data)} rows")

            data.reset_index(drop=True, inplace=True)
            logging.debug("Reset index of DataFrame")

            records = list(json.loads(data.T.to_json()).values())
            logging.info(f"Converted CSV to {len(records)} JSON records")

            return records
        except Exception as e:
            logging.error(f"Error converting CSV to JSON: {str(e)}")
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records
            logging.info(f"Connecting to MongoDB database: {database}, collection: {collection}")

            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)
            logging.info("Connected to MongoDB client")

            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]
            logging.debug(f"Selected database: {database}, collection: {collection}")

            # Insert the records into the MongoDB collection
            logging.info(f"Inserting {len(records)} records into MongoDB")
            self.collection.insert_many(records)
            logging.info(f"Successfully inserted {len(records)} records into MongoDB")

            return len(self.records)
        except Exception as e:
            logging.error(f"Error inserting data into MongoDB: {str(e)}")
            raise NetworkSecurityException(e, sys)

if __name__ == "__main__":
    try:
        logging.info("Starting push_data.py script")
        # Get the file path from the command line argument
        file_path = "Network_Data\\phisingData.csv"
        database = "network_security"
        collection = "phishing_data"
        logging.info(f"Configuration: file_path={file_path}, database={database}, collection={collection}")

        # Create an instance of the NetworkSecurityData class
        logging.info("Creating NetworkSecurityData instance")
        network_security_data = NetworkSecurityData(file_path)

        # Convert the CSV file to JSON
        logging.info("Converting CSV to JSON")
        records = network_security_data.csv_to_json_converter(file_path)

        # Log the number of records
        logging.info(f"Generated {len(records)} JSON records")

        # Insert the records into MongoDB
        logging.info("Inserting records into MongoDB")
        inserted_count = network_security_data.insert_data_mongodb(records, database, collection)

        logging.info(f"Successfully inserted {inserted_count} records into MongoDB")
        print(f"Inserted {inserted_count} records into MongoDB.")

    except Exception as e:
        logging.error(f"Script failed: {str(e)}")
        raise NetworkSecurityException(e, sys)