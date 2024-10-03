import duckdb
import requests

from .constants import TAXI_TRIPS_TEMPLATE_FILE_PATH, TAXI_ZONES_FILE_PATH, DUCK_DB_PATH

from dagster import asset


@asset
def taxi_trips_file() -> None:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    month_to_fetch = "2023-03"
    get_march_taxi_trips_api_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    raw_trips_parquet = requests.get(get_march_taxi_trips_api_url)

    with open(
        TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips_parquet.content)


@asset
def taxi_zones_file() -> None:
    """
    The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    get_zone_api_url = (
        "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )
    raw_zones_csv = requests.get(get_zone_api_url)

    with open(TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones_csv.content)


@asset(deps=["taxi_trips_file"])
def taxi_trips() -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database
    """
    sql_query = f"""
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from '{TAXI_TRIPS_TEMPLATE_FILE_PATH.format("2023-03")}'
        );
    """

    conn = duckdb.connect(DUCK_DB_PATH)
    conn.execute(sql_query)


@asset(deps=["taxi_zones_file"])
def taxi_zones() -> None:
    """
    The raw taxi zones table, loaded into a DuckDB database
    """
    sql_query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone,
            borough,
            the_geom as geometry
          from '{TAXI_ZONES_FILE_PATH}'
        );
    """

    conn = duckdb.connect(DUCK_DB_PATH)
    conn.execute(sql_query)
