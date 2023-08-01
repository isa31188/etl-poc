import os, boto3, logging, requests
from botocore.exceptions import ClientError
from zipfile import ZipFile

# For input arguments
import sys
from awsglue.utils import getResolvedOptions


def download_data(url: str, s3_destination_bucket: str) -> str:
    
    s3_client = boto3.client('s3')
    
    file_name = url.split("/")[-1]
    object_name = file_name
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
    }

    logging.info("Requesting data from URL")
    with open(file_name, 'wb') as f:
        req = requests.get(url, headers=headers)  
        f.write(req.content)
        
    # Upload the file to S3
    logging.info("Dumping data file to S3")
    s3_client.upload_file(file_name, s3_destination_bucket, object_name)

    os.remove(file_name)
    return object_name

def decompress_data(object_name: str, s3_bucket: str) -> str:
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
    }

    s3_client = boto3.client('s3')
    
    file_name = object_name

    # Download the file from S3
    logging.info("Downloading the zip file")
    s3_client.download_file(s3_bucket, object_name, file_name)
    
    # Unzip locally
    logging.error("Decompressing locally")
    with ZipFile(file_name, 'r') as zip_file:
        zip_file.extractall(".")

    for fname in os.listdir(path='.'):
        if fname.endswith(".csv"):
            unzipped_file_name = fname

    clean_unzipped_file_name = unzipped_file_name.replace(" ","-")
    unzipped_object_name = clean_unzipped_file_name
    
    os.rename(unzipped_file_name, clean_unzipped_file_name)
        
    logging.error(os.listdir(path='.'))

    # Upload the unzipped file to S3
    logging.error("Uploading the unzip file to S3")
    s3_client.upload_file(clean_unzipped_file_name, s3_bucket, unzipped_object_name)

    return unzipped_object_name

if __name__ == "__main__":

    #parser = argparse.ArgumentParser()
    #parser.add_argument('--url', type=str, required=True, help="URL for extracting data.")
    #parser.add_argument('--s3_destination_bucket', type=str, required=True, help="S3 bucket where to dump data.")

    #args = parser.parse_args()
    args = getResolvedOptions(sys.argv, ['url', 's3_destination_bucket'])

    object_name = download_data(url=args["url"], s3_destination_bucket=args["s3_destination_bucket"])
    unzipped_object_name = decompress_data(object_name, args["s3_destination_bucket"])
