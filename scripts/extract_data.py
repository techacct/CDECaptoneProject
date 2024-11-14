import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import NoCredentialsError

def fetch_api_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def store_raw_data_parquet(data, output_path):
    table = pa.Table.from_pandas(pd.DataFrame(data))
    pq.write_table(table, output_path)

def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"File uploaded successfully to s3://{bucket_name}/{s3_key}")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

def main():
    url = "https://restcountries.com/v3.1/all"
    raw_data = fetch_api_data(url)
    output_path = "/workspaces/CDECaptoneProject/data/output/raw_data.parquet"
    
    # Store data locally in Parquet format
    store_raw_data_parquet(raw_data, output_path)
    print("Data stored successfully in Parquet format.")
    
    # Upload to S3
    bucket_name = "travel-agency-data-lakes"  # Replace with your S3 bucket name
    s3_key = "arn:aws:s3:::travel-agency-data-lakes/raw_data.parquet"  # Replace with the desired S3 key path
    upload_to_s3(output_path, bucket_name, s3_key)

if __name__ == "__main__":
    main()
