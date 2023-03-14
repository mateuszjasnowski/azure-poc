"""
Azure function to collect public transport vehicles location and save to blob storge.
Module to handle html request and proceed with data workflow
"""

import logging
from requests.exceptions import MissingSchema
from requests.exceptions import ConnectTimeout
from requests.exceptions import ConnectionError as RequestsError

import azure.functions as func
from .location import Locations


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Reciving http requests,
    collecting data file from remote location
    saving to blob storage
    responding to with html response
    """
    logging.info("Recived html request. Executing actions ...")

    data_url = req.params.get("data")

    if not data_url:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            data_url = req_body.get("data")

    return_msg = {
        "api_url": data_url,
        "message": "No 'data' argument recived " + "Pass 'data' argument with url",
        "status": "failed",
    }
    status_code = 400

    # Recived data_url
    if data_url:
        try:
            new_location_data = Locations(data_url).azure_blob_save()
        except MissingSchema as error:
            return_msg["message"] = error
        except ConnectionError as error:
            return_msg["message"] = "Connection error"
            return_msg["error"] = error
        except RequestsError:
            return_msg["message"] = "Connection timeout"

        else:
            if new_location_data:
                return_msg[
                    "message"
                ] = "Successfully saved file in blob storage from given url."
                return_msg["status"] = "success"
                return_msg["new_file_name"] = new_location_data
                status_code = 200

            else:
                return_msg["message"] = "unkown error"
                return_msg["status"] = "error"

    return func.HttpResponse(str(return_msg), status_code=status_code)
