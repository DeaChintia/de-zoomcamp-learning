from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

ROOT_DIRECTORY = "d:/destuffs/Learning/DataEngineerAndDataWarehouse/de_zoomcamp/week_2_workflow_orchestration"


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = Path(f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet")
    gcs_block = GcsBucket.load("zoomcamp-bucket")
    gcs_block.get_directory(from_path=gcs_path.as_posix(), local_path=Path(ROOT_DIRECTORY))
    return gcs_path


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    path = Path(f"{ROOT_DIRECTORY}/{path.as_posix()}")
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write Dataframe to Big Query"""
    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")

    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="de-zoomcamp-388707",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
