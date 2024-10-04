from dagster import ScheduleDefinition
from ..jobs import trip_update_job, weekly_trip_update_job

trip_update_schedule = ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="0 0 5 * *",
)

weekly_update_schedule = ScheduleDefinition(
    job=weekly_trip_update_job,
    cron_schedule="0 0 * * 1",
)
