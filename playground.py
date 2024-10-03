import duckdb
import pandas as pd


conn = duckdb.connect(database="dagster_university/data/staging/data.duckdb")
current_date_str = "2023-03-01"
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
metric_list = conn.execute(current_week_metrics_sql).fetchall()
result_pdf = pd.DataFrame(
    metric_list,
    columns=[
        "week",
        "period",
        "num_trips",
        "passenger_count",
        "total_amount",
        "trip_distance",
    ],
)
print(result_pdf)
