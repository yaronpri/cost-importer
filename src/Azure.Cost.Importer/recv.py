import sys, time, logging
import asyncio, os
import json
from datetime import datetime, timedelta
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.storage.blob.aio import BlobServiceClient
import pandas as pd
import base64
import requests
import hmac
import hashlib

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)   
logger.addHandler(logging.StreamHandler(sys.stdout))


#blob source details
blob_service_connection = os.environ["BLOB_STORE_CONNECTIONSTRING"]
blob_service_client = BlobServiceClient.from_connection_string(blob_service_connection)
preprocess_container_name = os.environ["BLOB_STORE_CONTAINERNAME"]
container_uri = blob_service_client.primary_endpoint + preprocess_container_name

#checkpoint blob details
checkpoint_store_connection = os.environ["BLOB_CHECKPOINT_CONNECTIONSTRING"]
checkpoint_store_container = os.environ["BLOB_CHECKPOINT_CONTAINERNAME"]

#EventHub details
event_hub_connection = os.environ["EVENTHUB_CONNECTIONSTRING"]
event_hub_name = os.environ["EVENTHUB_NAME"]
consumer_group_name = os.environ["EVENTHUB_CONSUMERGROUP"]

azure_log_customer_id = os.environ["LOG_ANALYTIC_CUSTOMER_ID"]
azure_log_shared_key = os.environ["LOG_ANALYTIC_KEY"]
table_name = 'costimporter'

def build_signature(customer_id, shared_key, date, content_length, method, content_type, resource):
    """Returns authorization header which will be used when sending data into Azure Log Analytics"""
    
    x_headers = 'x-ms-date:' + date
    string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
    bytes_to_hash = bytes(string_to_hash, 'UTF-8')
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode('utf-8')
    authorization = "SharedKey {}:{}".format(customer_id,encoded_hash)
    return authorization

def post_data(customer_id, shared_key, body, log_type):
    """Sends payload to Azure Log Analytics Workspace
    
    Keyword arguments:
    customer_id -- Workspace ID obtained from Advanced Settings
    shared_key -- Authorization header, created using build_signature
    body -- payload to send to Azure Log Analytics
    log_type -- Azure Log Analytics table name
    """
    
    method = 'POST'
    content_type = 'application/json'
    resource = '/api/logs'
    rfc1123date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    content_length = len(body)
    signature = build_signature(customer_id, shared_key, rfc1123date, content_length, method, content_type, resource)

    uri = 'https://' + customer_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

    headers = {
        'content-type': content_type,
        'Authorization': signature,
        'Log-Type': log_type,
        'x-ms-date': rfc1123date
    }

    response = requests.post(uri,data=body, headers=headers)
    if (response.status_code >= 200 and response.status_code <= 299):
        logging.info('Accepted payload:' + body)
    else:
        logging.error("Unable to Write: " + format(response.status_code))

async def on_event(partition_context, event):
    try:
        start = time.time()

        new_event_json = json.loads(event.body_as_str(encoding='UTF-8'))     
        full_blob_name = new_event_json[0]["data"]["url"]
        
        start_substring = full_blob_name.index(container_uri) + len(container_uri) + 1
        blob_name = full_blob_name[start_substring:len(full_blob_name)]                
        if (".csv" in blob_name):
            blob_client = blob_service_client.get_blob_client(container=preprocess_container_name, blob=blob_name)
            with open("cost.csv", "wb") as my_blob:
                stream = await blob_client.download_blob()
                await stream.readinto(my_blob)        
            
            dataframe_blobdata = pd.read_csv("cost.csv")
            lastday = datetime.today() - timedelta(days=2) #we would like to filter the new cost rows 
            dataframe_blobdata['Date']= pd.to_datetime(dataframe_blobdata['Date'], format='%m/%d/%Y')
            df_filtered = dataframe_blobdata.loc[dataframe_blobdata['Date'] > lastday]

            for index, row in df_filtered.iterrows():
                resourceid = row['ResourceId']
                effectiveprice = row['EffectivePrice']
                date = str(row['Date'])
                currency = row['BillingCurrencyCode']
                productname = row['ProductName']
                consumedservice = row['ConsumedService']
                resourcegroup = row['ResourceGroup']
                subscription = row['SubscriptionName']
                data = {
                    "resourceid" : resourceid,
                    "effectiveprice": effectiveprice,
                    "date": date,
                    "currency": currency,
                    "productname" : productname,
                    "consumedservice": consumedservice,
                    "resourceGroup": resourcegroup,
                    "subscription": subscription
                }
                data_json = json.dumps(data)
                post_data(azure_log_customer_id, azure_log_shared_key, data_json, table_name)
                logger.info('Processing row number ' + str(index) + " " + datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
      
        await partition_context.update_checkpoint(event)
        end = time.time()
       
        logger.info("End Azure-Cost-Importer event " + blob_name + " " + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + " took (sec) " + str(end-start))
    except Exception as e:
        logger.error("Error - ", e.args[0])        
        await partition_context.update_checkpoint(event)

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(checkpoint_store_connection, checkpoint_store_container)
    
    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(event_hub_connection, consumer_group=consumer_group_name, eventhub_name=event_hub_name, checkpoint_store=checkpoint_store)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        logger.info("Start Azure-Cost-Importer listening")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())