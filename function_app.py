import logging
import os
import random
import tempfile
from typing import List, Tuple, Optional
from dotenv import load_dotenv
import numpy as np
import oracledb
import azure.functions as func
from azure.storage.blob import BlobServiceClient

# Type Aliases
BlobFile = dict[str, str]  # Type alias for a dictionary containing blob and local file paths
DataTuple = Tuple[str, str, float, int, str, float, int]  # Type alias for a tuple representing data rows

# Load environment variables
load_dotenv()

# Define constants and paths
TEMP_DIR = tempfile.gettempdir()
CONFIG_DIR = os.path.join(TEMP_DIR, os.environ.get("CONFIG_DIR", "config"))
WALLET_LOCATION = os.path.join(TEMP_DIR, os.environ.get("WALLET_LOCATION", "wallet"))

# Ensure the wallet directory exists
os.makedirs(WALLET_LOCATION, exist_ok=True)

# Define wallet file paths
PEM_PATH = os.path.join(WALLET_LOCATION, 'ewallet.pem')
TNS_PATH = os.path.join(WALLET_LOCATION, 'tnsnames.ora')

# Initialize the Function App
app = func.FunctionApp()

@app.schedule(schedule="*/10 * * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def generate_usage_data(myTimer: func.TimerRequest) -> None:
    """Function triggered by timer to generate and insert sample data into the database."""
    if myTimer.past_due:
        logging.warning('The timer is past due!')

    # Download wallet files and establish DB connection
    download_wallet_files()
    connection = get_db_connection()

    if connection:
        data_tuples = generate_sample_data_tuples(10)  # Generate 10 rows of sample data
        insert_data(connection, data_tuples)
        connection.close()

def download_wallet_files() -> None:
    """Downloads Oracle wallet files from Azure Blob Storage to the temporary directory."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
        files_to_download: List[BlobFile] = [
            {"blob": "ewallet.pem", "local": PEM_PATH},
            {"blob": "tnsnames.ora", "local": TNS_PATH}
        ]
        container_name = os.getenv("CONFIG_CONTAINER")

        for file_info in files_to_download:
            download_file(blob_service_client, container_name, file_info["blob"], file_info["local"])

    except Exception as e:
        logging.error(f"Error downloading files from Azure Blob Storage: {e}")

def download_file(blob_service_client: BlobServiceClient, container_name: str, blob_name: str, local_path: str) -> None:
    """Helper function to download a file from Azure Blob Storage."""
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(local_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    logging.info(f"Downloaded {blob_name} to {local_path}")

def get_db_connection() -> Optional[oracledb.Connection]:
    """Establishes a connection to the Oracle database using downloaded wallet files."""
    try:
        connection = oracledb.connect(
            config_dir=WALLET_LOCATION,
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            dsn=os.getenv("DSN"),
            wallet_location=WALLET_LOCATION,
            wallet_password=os.getenv("WALLET_PASSWORD")
        )
        return connection
    except oracledb.DatabaseError as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

def generate_sample_data_tuples(n_samples: int) -> List[DataTuple]:
    """Generates sample water consumption data as tuples."""
    time_of_day = np.random.choice(['morning', 'afternoon', 'evening', 'night'], size=n_samples)
    season = np.random.choice(['spring', 'summer', 'fall', 'winter'], size=n_samples)
    temperature = np.random.normal(loc=20, scale=5, size=n_samples)
    household_size = np.random.randint(1, 6, size=n_samples)
    day_of_week = np.random.choice(['weekday', 'weekend'], size=n_samples)

    normal_consumption = (household_size * 50) + np.random.normal(loc=0, scale=10, size=n_samples)
    anomalies = np.random.choice([True, False], size=n_samples, p=[0.05, 0.95])
    anomalous_consumption = normal_consumption.copy()
    anomalous_consumption[anomalies] += np.random.choice([0, 250], size=np.sum(anomalies))

    return [
        (
            time_of_day[i].item(),
            season[i].item(),
            temperature[i].item(),
            household_size[i].item(),
            day_of_week[i].item(),
            anomalous_consumption[i].item(),
            int(anomalies[i])  # Convert boolean to int (0 or 1)
        )
        for i in range(n_samples)
    ]

def insert_data(connection: oracledb.Connection, data_tuples: List[DataTuple]) -> None:
    """Inserts sample data into the water consumption database."""
    insert_sql = """
        INSERT INTO water_consumption_data (
            time_of_day, season, temperature, household_size, day_of_week, water_consumption, is_anomaly
        ) VALUES (:1, :2, :3, :4, :5, :6, :7)
    """
    cursor = connection.cursor()
    cursor.executemany(insert_sql, data_tuples)
    connection.commit()
    cursor.close()

    logging.info(f"Inserted {len(data_tuples)} rows into the database.")
