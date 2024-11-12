import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def fetch_api_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def store_raw_data_parquet(data, output_path):
    table = pa.Table.from_pandas(pd.DataFrame(data))
    pq.write_table(table, output_path)

def main():
    url = "https://restcountries.com/v3.1/all"
    raw_data = fetch_api_data(url)
    output_path = "/workspaces/CDECaptoneProject/data/output/raw_data.parquet"
    store_raw_data_parquet(raw_data, output_path)
    print("Data stored successfully in Parquet format.")

if __name__ == "__main__":
    main()
