from chalice import Chalice
from apify_client import ApifyClient
import json
import boto3
from datetime import datetime, timedelta
import traceback
import pandas as pd
import os
import concurrent.futures
from io import BytesIO


app = Chalice(app_name='apify-instagram')

def get_last_timestamp_from_csv(bucket, file_key):
    s3_client = boto3.client('s3')

    try:
        obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        data = obj['Body'].read()
        df = pd.read_csv(BytesIO(data))
        last_timestamp = df['Post_timestamp'].iloc[0]
        return last_timestamp

    except Exception as e:
        print(f"Error occurred: {e}")
        return None


def process_brand(brand, from_date, to_date, client,s3, bucket_name, folder_path):
    try:
        print("Get apify data from brand {}, from {} to {}".format(brand, from_date, to_date))
        url = "https://www.instagram.com/" + brand + "/?hl=en"
    # Prepare the Actor input
        run_input = {
                "directUrls": [url],
                "fromDate": from_date,
                "resultsType": "posts",
                "searchLimit": 1,
                "searchType": "hashtag",
                "untilDate": to_date,
                "extendOutputFunction": """async ({ data, item, helpers, page, customData, label }) => {
            return item;
            }""",
                "extendScraperFunction": """async ({ page, request, label, response, helpers, requestQueue, logins, addProfile, addPost, addLocation, addHashtag, doRequest, customData, Apify }) => {
            
            }""",
                "customData": {},
            }

            # Run the Actor and wait for it to finish
        run = client.actor("apify/instagram-scraper").call(run_input=run_input)
        data_variable = client.dataset(run["defaultDatasetId"]).list_items().items
        payload = {
                "data": data_variable
            }
        data_length = str(len(payload['data']))
        print("Get {} rows of data".format(data_length))

        # Convert payload to a valid JSON string
        fileName = brand + "-" + from_date + "-" +to_date + "-"  + ".json"
        data = json.dumps(payload).encode('UTF-8')
        object_key = folder_path + fileName

        s3.put_object(Body = data, Bucket = bucket_name, Key = object_key)
        log = "file upload into s3 {}/{} successfully".format(bucket_name,object_key)
        print(log)
        return {'message': log}
    except Exception as e:
        # Log the error and continue processing other brands
        error_message = f"Error processing {brand}: {str(e)}"
        traceback.print_exc()  # Print the traceback for debugging
        print(error_message)



@app.lambda_function()
def get_and_upload_data(event, context):
    bucket = os.environ.get('bucket')
    folder_path =  os.environ.get('folder_path')
    file_key = os.environ.get('file_key')
    api_tocken =  os.environ.get('api_tocken')
    
    last_timestamp = get_last_timestamp_from_csv(bucket, file_key)
    from_date = last_timestamp 
    to_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # GET DATA From APIFY
    client = ApifyClient(api_tocken)
    session = boto3.Session()
    s3 = session.client('s3')

    #competetors_lst = [ "monsterenergy"]
    #competetors_lst = ["waterloosparkling","drinkspindrift", "perriercanada"]
    #competetors_lst = ["sanpellegrinoca","purelifecanada","montelliercanada"]
    #competetors_lst = ["originwater","lacroixwater","bublywater"]
    competetors_lst = ["waterloosparkling","purelifecanada","lacroixwater", "bublywater", "perriercanada", "monsterenergy", "sanpellegrinoca","drinkspindrift", "montelliercanada", "originwater"] 
 
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit tasks for each brand
        for brand in competetors_lst:
            executor.submit(process_brand, brand, from_date, to_date, client, s3, bucket, folder_path)

    return {'message': 'Processing initiated for all brands'}
