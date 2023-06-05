#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = "data.parquet"
    os.system(f"wget {url} -O {parquet_name}")

    parquet_file = pq.ParquetFile(parquet_name)

    metadata = parquet_file.metadata
    schema = parquet_file.schema.to_arrow_schema()
    metadata = schema.pandas_metadata["columns"]

    # Extract column names and data types
    columns = [item["name"] for item in metadata]
    dtypes = {item["name"]: item["numpy_type"] for item in metadata}

    # Create an empty DataFrame with columns and data types
    df = pd.DataFrame(columns=columns).astype(dtypes)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    for batch in parquet_file.iter_batches(batch_size=100000):
        t_start = time()
        batch_df = batch.to_pandas()
        batch_df.to_sql(name=table_name, con=engine, if_exists="append")
        t_end = time()
        print("inserted another chunk... took %.3f second" % (t_end - t_start))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Parquet file into Postgres")
    parser.add_argument("--user", help="user name for PostgreSQL")
    parser.add_argument("--password", help="password for PostgreSQL")
    parser.add_argument("--host", help="host for PostgreSQL")
    parser.add_argument("--port", help="port for PostgreSQL")
    parser.add_argument("--db", help="database name for PostgreSQL")
    parser.add_argument("--table_name", help="name of the table where results will be written")
    parser.add_argument("--url", help="url of the parquet file")

    args = parser.parse_args()
    main(args)