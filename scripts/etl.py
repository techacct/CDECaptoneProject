import pandas as pd
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import NoCredentialsError
import io
from decimal import Decimal
import json

# AWS clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Parameters
BUCKET_NAME = "travel-agency-data-lakes"  # Your S3 bucket name
RAW_PARQUET_KEY = "arn:aws:s3:::travel-agency-data-lakes/raw_data.parquet"  # Key of the raw Parquet file in S3
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
    def safe_currency_name(currencies):
        """Safely extract currency name."""
        if currencies:
            try:
                return list(currencies.values())[0].get("name")
            except (IndexError, AttributeError):
                return None
        return None

    def safe_currency_code(currencies):
        """Safely extract currency code."""
        if currencies:
            try:
                return list(currencies.keys())[0]
            except (IndexError, AttributeError):
                return None
        return None

    def safe_currency_symbol(currencies):
        """Safely extract currency symbol."""
        if currencies:
            try:
                return list(currencies.values())[0].get("symbol")
            except (IndexError, AttributeError):
                return None
        return None

    def safe_country_code(idd):
        """Safely extract country code from the 'idd' field."""
        if isinstance(idd, dict):  # Ensure 'idd' is a dictionary
            root = idd.get("root")
            suffixes = idd.get("suffixes")
            if root and isinstance(suffixes, list) and len(suffixes) > 0:
                return f"{root}{suffixes[0]}"
        return None

    def safe_capital(x):
        """Safely extract capital from the list or array."""
        if isinstance(x, (list, tuple)) and len(x) > 0:
            return x[0]  # Return the first element if it's a list/tuple
        return None  # Return None if not a valid list/tuple or empty

    def safe_continents(x):
        """Safely extract the first continent from a list or array."""
        if isinstance(x, (list, tuple)) and len(x) > 0:
            return x[0]  # Return the first element if it's a list/tuple
        return None  # Return None if not a valid list/tuple or empty

    # Apply the transformation logic to each row of the DataFrame
    transformed = data.apply(lambda row: {
        "id": row["country_name"],  # Use country_name as the partition key (id)
        "country_name": row["country_name"],
        "independent": row["independent"],
        "un_member": row["un_member"],
        "start_of_week": row["start_of_week"],
        "official_name": row["official_name"],
        "common_native_name": row["common_native_name"],
        "currency_code": row["currency_code"],
        "currency_name": row["currency_name"],
        "currency_symbol": row["currency_symbol"],
        "country_code": row["country_code"],
        "capital": safe_capital(row["capital"]),
        "region": row["region"],
        "subregion": row["subregion"],
        "languages": row["languages"],  # Languages may need further processing if you want to clean them up
        "area": row["area"],
        "population": row["population"],
        "continents": safe_continents(row["continents"])
    }, axis=1)
    return transformed

def convert_floats_to_decimal(obj):
    """Recursively convert all float values in the dictionary to Decimal."""
    if isinstance(obj, float):
        return Decimal(str(obj))  # Convert float to Decimal
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj

def load_to_dynamodb(table_name, data):
    """Load the transformed data into DynamoDB."""
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for index, row in data.iterrows():
            # Convert row to JSON, then load to a Python dictionary
            item = json.loads(row.to_json())
            # Convert any float values to Decimal
            item = convert_floats_to_decimal(item)

            # Ensure that the partition key (and sort key if needed) are present
            # For example, assume 'id' is the partition key and 'timestamp' is the sort key
            if 'id' not in item:  # Replace 'id' with your actual partition key name
                raise ValueError(f"Missing partition key: 'id' in item: {item}")
            
            if 'timestamp' not in item:  # Replace 'timestamp' with your actual sort key name, if applicable
                raise ValueError(f"Missing sort key: 'timestamp' in item: {item}")

            # Add the item to the batch
            batch.put_item(Item=item)

def main():
    # Step 1: Extract
    raw_data = read_parquet_from_s3(BUCKET_NAME, RAW_PARQUET_KEY)

    # Step 2: Transform
    transformed_data = transform_data(raw_data)

    # Step 3: Load
    if transformed_data is not None:
        load_to_dynamodb(DYNAMO_TABLE_NAME, transformed_data)
    else:
        print("No transformed data to load into DynamoDB.")

if __name__ == "__main__":
    main()
