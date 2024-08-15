import logging
import requests
from azure.storage.blob import BlobServiceClient
import azure.functions as func

# Configuration
STORAGE_ACCOUNT_KEY = "fZ722WzZLOn8HqWNysUG9hnM6gfAUtv5t57uneRfad9w8J8kHa6rlLrkBV/EPjudVUHcI8s/h5xH+AStWAAXJw=="
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=rawdata001;AccountKey=fZ722WzZLOn8HqWNysUG9hnM6gfAUtv5t57uneRfad9w8J8kHa6rlLrkBV/EPjudVUHcI8s/h5xH+AStWAAXJw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "mst"
STORAGE_ACCOUNT_NAME = "rawdata001"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing HTTP request to upload CSV file.')

    try:
        # Extract start_date and end_date from the HTTP request parameters
        start_date = req.params.get('2024-01-01')
        end_date = req.params.get('2024-05-05')

        if not start_date or not end_date:
            return func.HttpResponse(
                "Please provide both start_date and end_date parameters in the format YYYY-MM-DD.",
                status_code=400
            )

        # Use start_date and end_date in your API request
        yahoo_finance_url = f"https://finance.yahoo.com/quote/CRM/history?period1={start_date}&period2={end_date}"

        # Fetch the data from Yahoo Finance
        response = requests.get(yahoo_finance_url)
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


