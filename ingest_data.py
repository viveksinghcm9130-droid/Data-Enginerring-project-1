import pandas as pd
from sqlalchemy import create_engine
import argparse
import os
import requests


def download_file(url, output_path):
    print(f"Downloading file from: {url}")

    response = requests.get(url, stream=True)

    if response.status_code != 200:
        raise Exception(f"Failed to download file. Status Code: {response.status_code}")

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)

    print(f"File downloaded: {output_path}")


def main(params):

    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # Identify parquet file extension
    if url.endswith('.parquet.gz'):
        parquet_name = 'output.parquet.gz'
    else:
        parquet_name = 'output.parquet'

    # Download parquet using requests
    download_file(url, parquet_name)

    # Read parquet file
    print("Reading parquet file...")

 

    # Read parquet file
    df = pd.read_parquet(parquet_name)

    # Create PostgreSQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Test connection
    engine.connect()

    # Print schema (useful before table creation)
    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    # Load dataframe to Postgres
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    # Validate row count
    query = f"SELECT COUNT(*) FROM {table_name};"
    result = pd.read_sql(query, con=engine)
    print(result)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='table where to write data')
    parser.add_argument('--url', required=True, help='url of the parquet file')

    args = parser.parse_args()

    main(args)
