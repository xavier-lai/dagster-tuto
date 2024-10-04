from dagster import asset, AssetExecutionContext

import plotly.express as px
import plotly.io as pio
import geopandas as gpd

import duckdb
import pandas as pd

from .constants import (
    MANHATTAN_STATS_FILE_PATH,
    MANHATTAN_MAP_FILE_PATH,
    DUCK_DB_PATH,
    DATE_FORMAT,
    TRIPS_BY_WEEK_FILE_PATH,
)

from ..partitions import weekly_partition
from datetime import datetime, timedelta
from typing import List
from dagster_duckdb import DuckDBResource


@asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_trips(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(MANHATTAN_STATS_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_zone.to_json())


@asset(
    deps=["manhattan_trips"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(
        trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color="num_trips",
        color_continuous_scale="Plasma",
        mapbox_style="carto-positron",
        center={"lat": 40.758, "lon": -73.985},
        zoom=11,
        opacity=0.7,
        labels={"num_trips": "Number of Trips"},
    )

    pio.write_image(fig, MANHATTAN_MAP_FILE_PATH)


@asset(deps=["taxi_trips", "taxi_zones"], partitions_def=weekly_partition)
def trips_by_week(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """Metrics of taxi trips by weeks"""
    current_week_date_str = context.partition_key
    current_week_metrics_sql = f"""
        select
            date_trunc('week', pickup_datetime) as week,
            '{current_week_date_str}' AS period,
            count(*) as num_trips,
            sum(passenger_count) AS passenger_count,
            sum(total_amount) AS total_amount,
            sum(trip_distance) AS trip_distance
        from trips
        where pickup_datetime >= '{current_week_date_str}' and pickup_datetime < '{current_week_date_str}'::date + interval '1 week'
        group by week
    """

    with database.get_connection() as conn:
        current_week_metrics_pdf: pd.DataFrame = conn.execute(
            current_week_metrics_sql
        ).fetch_df()

    try:
        existing_all_week_metric_pdf = pd.read_csv(TRIPS_BY_WEEK_FILE_PATH)
        existing_other_weeks_metric_pdf = existing_all_week_metric_pdf[
            existing_all_week_metric_pdf["period"] != current_week_date_str
        ]
        all_week_metric_pdf = pd.concat(
            [existing_other_weeks_metric_pdf, current_week_metrics_pdf]
        )
        all_week_metric_pdf.to_csv(TRIPS_BY_WEEK_FILE_PATH, index=False)
    except FileNotFoundError:
        current_week_metrics_pdf.to_csv(TRIPS_BY_WEEK_FILE_PATH, index=False)
