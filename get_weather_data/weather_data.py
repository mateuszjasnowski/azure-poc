""" Module to collect weather data from open weather api and save to blob storge """
import os
import csv
import io
import logging
from datetime import datetime
import requests

from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
from azure.core.exceptions import ResourceExistsError
from azure.core.exceptions import ResourceNotFoundError


class WeatherData:
    """Creating object containing weather data"""

    def __init__(self) -> None:
        """Inizialize object with given data file"""

        self.api_key = os.environ["OPEN_WEATHER_API_KEY"]

        self.api_url = os.environ["OPEN_WEATHER_API_URL"]
        self.connection_string = os.environ["AZURE_BLOB_CONNECTION_STRING"]
        self.container_name = os.environ["AZURE_BLOB_CONTAINER_NAME"]

        self.request_time = datetime.strftime(datetime.now(), "%d-%m-%y_%H-%M-%S")
        logging.info("Recived request at %s", self.request_time)

        self.cities = [
            {"name": "Wrocław", "lon": 51.107229588972274, "lat": 17.03649484873202},
            {"name": "Poznań", "lon": 52.40679153622946, "lat": 16.923980849540058},
            {"name": "Gdańsk", "lon": 54.352603073028355, "lat": 18.644018240738358},
            {"name": "Katowice", "lon": 50.26547260052914, "lat": 19.02161122578667},
            {"name": "Kraków", "lon": 50.06475601737979, "lat": 19.93963764829647},
            {"name": "Warszawa", "lon": 52.22906962953621, "lat": 21.02891789167521}
        ]

    def get_data(self, geo_lon: float, geo_lat: float) -> dict:
        """Get data from url and returns all collected records"""

        api_url = f"{self.api_url}?lat={geo_lat}&lon={geo_lon}&appid={self.api_key}&units=metric"
        api_data = requests.get(url=api_url, timeout=10)
        logging.info("%s", api_data)

        if api_data.status_code == 200:
            return api_data.json()

        logging.info("Empty api_data")
        return None

    def data_to_csv(self, api_data: dict, city_name: str) -> str:
        """ Collect only needed data from api call """
        if api_data:
            data = {
                "weather": api_data["weather"][0]["main"],
                "weather_desc": api_data["weather"][0]["description"],
                "temp": api_data["main"]["temp"],
                "feels_like": api_data["main"]["feels_like"],
                "pressure": api_data["main"]["pressure"],
                "humidity": api_data["main"]["humidity"],
                "visibility": api_data["visibility"],
                "wind_speed": api_data["wind"]["speed"],
                "wind_deg": api_data["wind"]["deg"],
                "clouds": api_data["clouds"]["all"],
                "dt": api_data["dt"],
                "city": city_name
            }

            io_output = io.StringIO()

            header = data.keys()

            csv_data = csv.DictWriter(io_output, header)
            #csv_data.writeheader()
            csv_data.writerow(data)

            return io_output.getvalue()
        return None

    def get_current_csv(self) -> str:
        """ Download latest csv from Blob storage to append new data """
        latest_file = "weather_latest.csv"

        try:
            blob = BlobClient.from_connection_string(
                conn_str = self.connection_string,
                container_name = self.container_name,
                blob_name = latest_file
            )
            my_blob = io.StringIO()
            blob_data = blob.download_blob(max_concurrency=1, encoding="utf-8")
            my_blob = blob_data.readall()
        except ResourceNotFoundError:
            header = "weather,weather_desc,temp,feels_like,\
pressure,humidity,visibility,wind_speed,wind_deg,clouds,dt,city\n"

            return header
        return my_blob

    def save_to_blob(self, blob_data: str) -> str:
        """Saving text data to blob storage at Azure"""
        file_name = f"weather_{self.request_time}.csv"
        #file_name = "weather_latest.csv"

        blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )

        try:
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name, blob=file_name
            )

            blob_client.upload_blob(blob_data, overwrite=True)
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

        weather_data = "".join(
            [self.data_to_csv(
                self.get_data(city.get("lat"),
                              city.get("lon")),
                              city.get("name"))
                              for city in self.cities
            ])

        if weather_data != "":

            result_data = self.get_current_csv() + weather_data

            save_result = self.save_to_blob(result_data)

            return save_result

        return False
