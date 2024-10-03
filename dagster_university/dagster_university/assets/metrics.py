from dagster import asset

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


@asset(deps=["taxi_trips", "taxi_zones"])
def trips_by_week(database: DuckDBResource) -> None:
    """Metrics of taxi trips by weeks"""
    current_date = datetime.strptime("2023-03-01", DATE_FORMAT)
    end_date = datetime.strptime("2023-04-01", DATE_FORMAT)

    all_week_metrics_list: List[pd.DataFrame] = []

    while current_date < end_date:
        current_date_str = current_date.strftime(DATE_FORMAT)
        current_week_metrics_sql = f"""
            select
                date_trunc('week', pickup_datetime) as week,
                strftime(date_trunc('week', pickup_datetime) + interval '6 days', '%Y-%m-%d') AS period,
                count(*) as num_trips,
                sum(passenger_count) AS passenger_count,
                sum(total_amount) AS total_amount,
                sum(trip_distance) AS trip_distance
            from trips
            where date_trunc('week', pickup_datetime) = date_trunc('week', '{current_date_str}'::date)
            group by week
        """

        with database.get_connection() as conn:
            current_week_metrics_list = conn.execute(
                current_week_metrics_sql
            ).fetchall()
        current_week_metrics_pdf = pd.DataFrame(
            current_week_metrics_list,
            columns=[
                "week",
                "period",
                "num_trips",
                "passenger_count",
                "total_amount",
                "trip_distance",
            ],
        )
        all_week_metrics_list.append(current_week_metrics_pdf)
        current_date += timedelta(days=7)

    weekly_metrics_pdf = pd.concat(all_week_metrics_list)
    weekly_metrics_pdf.to_csv(TRIPS_BY_WEEK_FILE_PATH, index=False)
