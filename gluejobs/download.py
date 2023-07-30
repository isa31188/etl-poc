
import requests, os, boto3, logging
from botocore.exceptions import ClientError
from zipfile import ZipFile

file_name = '2m-Sales-Records.zip'

bucket = "etl-poc-data-raw"
url = 'https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip'

object_name = file_name
s3_filepath = f"s3://{bucket}/{file_name}"

def download_data():

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
    }

    with open(local_filepath, 'wb') as f:
        req = requests.get(url, headers=headers)  
        f.write(req.content)
        
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

    os.remove(local_filepath)

if __name__ == "__main__":
    download_data()