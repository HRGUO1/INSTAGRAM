from apify_client import ApifyClient
import json
from datetime import date, datetime, timedelta
import calendar
import boto3

# Credentials
api_tocken = ''
aws_id = "AKIA6IMSXULXZG7KO5IU"
aws_key = "Kv1eFQBT4HWx9YdZY3xZyW/irIRYgHSrBXmolNWP"


# AWS Path
folder_path = "instagram/"
bucket = "s3-apify-instagram-raw-dta"

# time range
from_date = "2023-03-01"
to_date = "2023-11-10"


# GET DATA From APIFY
client = ApifyClient(api_tocken)
session = boto3.Session(
        aws_access_key_id = aws_id,
        aws_secret_access_key= aws_key
    )
s3 = session.client('s3')


competetors_lst = ["lacroixwater","bublywater","perriercanada","monsterenergy","sanpellegrinoca","waterloosparkling","drinkspindrift","purelifecanada","montelliercanada", "originwater"]

for brand in competetors_lst:
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

    # Convert payload to a valid JSON string
 #   fileName = brand + "-" + from_date + "-" + to_date + "-" + data_length + ".json"
    fileName = brand + "-" + from_date + ".json"
    data = json.dumps(payload).encode('UTF-8')
    object_key = folder_path + fileName

    s3.put_object(Body = data, Bucket = bucket, Key = object_key)
    print("file upload into s3 {}/{}".format(bucket, object_key))

