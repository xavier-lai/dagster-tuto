import requests

from .constants import TAXI_TRIPS_TEMPLATE_FILE_PATH, TAXI_ZONES_FILE_PATH

from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..partitions import monthly_partition


@asset(partitions_def=monthly_partition)
def taxi_trips_file(context: AssetExecutionContext) -> None:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
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


@asset(deps=["taxi_trips_file"], partitions_def=monthly_partition)
def taxi_trips(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.partition_key
    partition_month_str = partition_date_str[:-3]
    create_table_if_not_exist_query = f"""
    create table if not exists trips (
      vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
      rate_code_id double, payment_type integer, dropoff_datetime timestamp,
      pickup_datetime timestamp, trip_distance double, passenger_count double,
      total_amount double, partition_month varchar
      );
    """
    delete_current_existing_partition_query = f"""
    delete from trips where partition_month = '{partition_month_str}';
    """
    insert_trips_query = f"""
    insert into trips
    select
      cast(VendorID as string) as vendor_id,
      cast(PULocationID as string) as pickup_zone_id,
      cast(DOLocationID as string) as dropoff_zone_id,
      cast(RatecodeID as double) as rate_code_id,
      cast(payment_type as integer) as payment_type,
      cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
      cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
      cast(trip_distance as double) as trip_distance,
      cast(passenger_count as double) as passenger_count,
      cast(total_amount as double) as total_amount,
      '{partition_month_str}' as partition_month
    from '{TAXI_TRIPS_TEMPLATE_FILE_PATH.format(partition_month_str)}';
    """

    with database.get_connection() as conn:
        conn.execute(create_table_if_not_exist_query)
        conn.execute(delete_current_existing_partition_query)
        conn.execute(insert_trips_query)


@asset(deps=["taxi_zones_file"])
def taxi_zones(database: DuckDBResource) -> None:
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

    with database.get_connection() as conn:
        conn.execute(sql_query)
