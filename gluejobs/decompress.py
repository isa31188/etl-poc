import os, boto3, logging, requests
from botocore.exceptions import ClientError
from zipfile import ZipFile

file_name = '2m-Sales-Records.zip'
unzipped_file_name = file_name.replace(".zip",".csv")

bucket = "etl-poc-data-raw"
url = 'https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip'

object_name = file_name
unzipped_object_name = unzipped_file_name

def decompress_data():
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
    }
    
    s3_client = boto3.client('s3')

    # Download the file from S3
    logging.error("Downloading the zip file")
    s3_client.download_file(bucket, object_name, file_name)
    
    #with open(file_name, 'wb') as f:
    #    req = requests.get(url, headers=headers)  
    #    f.write(req.content)
        
    logging.error(os.listdir(path='.'))
    
    # Unzip locally
    logging.error("Decompressing locally")
    with ZipFile(file_name, 'r') as zip_file:
        zip_file.extractall(".")
       
    logging.error(os.listdir(path='.'))
   
    for fname in os.listdir(path='.'):
        if fname.endswith(".csv"):
            unzipped_file_name = fname

    new_unzipped_file_name = unzipped_file_name.replace(" ","-")
    unzipped_object_name = new_unzipped_file_name
    
    os.rename(unzipped_file_name, new_unzipped_file_name)
        
    logging.error(os.listdir(path='.'))

    # Upload the unzipped file to S3
    logging.error("Uploading the unzip file to S3")
    s3_client.upload_file(new_unzipped_file_name, bucket, unzipped_object_name)

    return

if __name__ == "__main__":
    decompress_data()