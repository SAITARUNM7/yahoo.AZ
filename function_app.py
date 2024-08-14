import azure.functions as func
import logging
import requests
from azure.storage.blob import BlobServiceClient

STORAGE_ACCOUNT_KEY = "8Q/SSR7RuavrPA5tFlQSrCNtvFwTbUZSg9742w+GgfB09h4hkSZKJtYrqvLfsPXq3irN+cMBoRmh+ASt6jlgwQ=="
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=rawdat1;AccountKey=8Q/SSR7RuavrPA5tFlQSrCNtvFwTbUZSg9742w+GgfB09h4hkSZKJtYrqvLfsPXq3irN+cMBoRmh+ASt6jlgwQ==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "yahoo-finance-data"
STORAGE_ACCOUNT_NAME = "rawdat1"

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing HTTP request to upload CSV file.')

    try:
        # Download the CSV file from the given URL
        response = requests.get('https://finance.yahoo.com/quote/CRM/history/')
        response.raise_for_status()  # Raise an error for bad responses

        # Connect to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # Create the container if it does not exist
        if not container_client.exists():
            container_client.create_container()

        # Upload the CSV file to Blob Storage
        blob_name = "uploaded_file.csv"  # Define the name of the blob in storage
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(response.content, blob_type="BlockBlob", overwrite=True)

        return func.HttpResponse("CSV file fetched and uploaded successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)
