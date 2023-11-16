import json
import boto3
from datetime import datetime, timedelta
import calendar
from apify_client import ApifyClient
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

# AWS Path
bucket = "s3-apify-instagram-raw-dta"
folder_path ='Update/'

# Initialize the ApifyClient with your API token
api_tocken = ''
client = ApifyClient(api_tocken)
aws_id = ""
aws_key = "/"

session = boto3.Session(
    aws_access_key_id=aws_id,
    aws_secret_access_key=aws_key
)
s3 = session.client('s3')

competitors_lst = ["lacroixwater", "bublywater", "perriercanada", "monsterenergy", "sanpellegrinoca","waterloosparkling", "drinkspindrift", "purelifecanada", "montelliercanada", "originwater"]

def get_last_timestamp_from_csv(bucket, file_key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        data = obj['Body'].read()
        df = pd.read_csv(BytesIO(data))
        last_timestamp = df['Post_timestamp'].iloc[0]
        last_date = datetime.strptime(last_timestamp, '%Y-%m-%d')
        next_day = last_date + timedelta(days=1)
        next_day = next_day.strftime('%Y-%m-%d')
        return next_day

    except Exception as e:
        print(f"Error occurred: {e}")
        return None

time_file_key = 'Processed_data/latest_row.csv'
last_timestamp = get_last_timestamp_from_csv(bucket, time_file_key)

# Time range
from_date = last_timestamp
d = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
to_date = d

for brand in competitors_lst:
    print("Get apify data from brand {}, from {} to {}".format(brand,from_date,to_date))
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
    fileName = f"{brand}-{from_date}-{to_date}.json"
    object_key = f"{folder_path}{fileName}"

    # Check if there is data to upload
    if len(payload['data']) >= 1:
        data = json.dumps(payload).encode('UTF-8')
        s3.put_object(Body=data, Bucket=bucket, Key=object_key)
        print(f"File uploaded into s3 {bucket}/{object_key}")
    else:
        print(f"No file uploaded for {brand}, no data available.")

