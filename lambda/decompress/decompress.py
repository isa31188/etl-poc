
import requests, os, boto3
from zipfile import ZipFile

file_name = '2m-Sales-Records.zip'

bucket = "etl-poc-data-raw"
url = 'https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip'

local_filepath = file_name
s3_filepath = f"s3://{bucket}/{file_name}"

def lambda_handler(event, context):

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
    }

    s3 = boto3.resource('s3')

    with open(local_filepath, 'wb') as f:
        req = requests.get(url, headers=headers)  
        f.write(req.content)
        
    s3.Bucket(bucket).upload_file(s3_filepath, file_name)

    os.remove(local_filepath)

# For local testing
#if __name__ == "__main__":
#    lambda_handler("","")