import pandas as pd
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import NoCredentialsError
import io
import json

# AWS clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Parameters
BUCKET_NAME = "travel-agency-data-lakes"  # Your S3 bucket name
RAW_PARQUET_KEY = "raw_data.parquet"  # Key of the raw Parquet file in S3
DYNAMO_TABLE_NAME = "analytics_data"  # DynamoDB table name

def read_parquet_from_s3(bucket_name, file_key):
    """Read Parquet file directly from S3 into a Pandas DataFrame."""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        parquet_data = response["Body"].read()
        buffer = io.BytesIO(parquet_data)
        table = pq.read_table(buffer)
        print(f"Parquet file read successfully from s3://{bucket_name}/{file_key}")
        return table.to_pandas()
    except NoCredentialsError:
        print("AWS credentials not available.")
        raise

def transform_data(data):
    """Transform raw data into a structured format for analytics."""
    transformed = pd.DataFrame({
        "country_name": data["name"].apply(lambda x: x.get("common")),
        "independent": data["independent"],
        "un_member": data["unMember"],
        "start_of_week": data["startOfWeek"],
        "official_name": data["name"].apply(lambda x: x.get("official")),
        "common_native_name": data["translations"].apply(lambda x: x.get("official") if x else None),
        "currency_code": data["currencies"].apply(lambda x: list(x.keys())[0] if x else None),
        "currency_name": data["currencies"].apply(lambda x: list(x.values())[0].get("name") if x else None),
        "currency_symbol": data["currencies"].apply(lambda x: list(x.values())[0].get("symbol") if x else None),
        "country_code": data["idd"].apply(lambda x: f"{x['root']}{x['suffixes'][0]}" if x and x['root'] and x['suffixes'] else None),
        "capital": data["capital"].apply(lambda x: x[0] if x else None),
        "region": data["region"],
        "subregion": data["subregion"],
        "languages": data["languages"].apply(lambda x: list(x.values()) if x else None),
        "area": data["area"],
        "population": data["population"],
        "continents": data["continents"].apply(lambda x: x[0] if x else None),
    })
    return transformed

def load_to_dynamodb(table_name, data):
    """Load transformed data into DynamoDB."""
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for _, row in data.iterrows():
            batch.put_item(Item=json.loads(row.to_json()))
    print("Data loaded into DynamoDB successfully!")

def main():
    # Step 1: Extract
    raw_data = read_parquet_from_s3(BUCKET_NAME, RAW_PARQUET_KEY)

    # Step 2: Transform
    transformed_data = transform_data(raw_data)

    # Step 3: Load
    load_to_dynamodb(DYNAMO_TABLE_NAME, transformed_data)

if __name__ == "__main__":
    main()
