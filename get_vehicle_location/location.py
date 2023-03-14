""" Module to collect public transport vehicles location and save to blob storge """
import os
import csv
import io
import logging
from datetime import datetime
import requests

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError


class Locations:
    """Creating object containing public transport vehicles location"""

    def __init__(self, url: str) -> None:
        """Inizialize object with given data file"""
        self.api_url = url
        self.connection_string = os.environ["AZURE_BLOB_CONNECTION_STRING"]
        self.container_name = os.environ["AZURE_BLOB_CONTAINER_NAME"]

        self.request_time = datetime.strftime(datetime.now(), "%d-%m-%y_%H-%M-%S")
        logging.info("Recived request at %s", self.request_time)

    def get_data(self) -> list:
        """Get data from url and returns all collected records"""

        api_data = requests.get(url=self.api_url, timeout=10).json()
        logging.info("%s", api_data)

        if api_data["success"]:
            return api_data["result"]["records"]

        logging.info("Empty api_data")
        return None

    def data_to_csv(self, data: list) -> str:
        """Convert dict with data to csv format"""

        io_output = io.StringIO()

        header = data[0].keys()

        csv_data = csv.DictWriter(io_output, header)
        csv_data.writeheader()
        csv_data.writerows(data)

        return io_output.getvalue()

    def save_to_blob(self, blob_data: str) -> str:
        """Saving text data to blob storage at Azure"""
        file_name = f"vehicle_location_{self.request_time}.csv"

        blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )

        try:
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name, blob=file_name
            )

            blob_client.upload_blob(blob_data)
            logging.info("Saving data to blob")
        except ResourceExistsError:
            logging.error(
                "Failed to save %s to blob storage. (File already exists)", file_name
            )
            return False
        logging.info("Succesfuly saved file %s", file_name)
        return file_name

    def azure_blob_save(self) -> bool:
        """Perform data download and save in Azure Blob"""

        api_data = self.get_data()

        if api_data:
            api_data = self.data_to_csv(api_data)
            save_result = self.save_to_blob(api_data)

            return save_result

        return False
