from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition, weekly_partition

trips_by_week = AssetSelection.assets("trips_by_week")
adhoc_request_asset = AssetSelection.assets("adhoc_request")

adhoc_request_job = define_asset_job(
    name="adhoc_request_job", selection=adhoc_request_asset
)

trip_update_job = define_asset_job(
    partitions_def=monthly_partition,
    name="trip_update_job",
    selection=AssetSelection.all() - trips_by_week - adhoc_request_asset,
)

weekly_trip_update_job = define_asset_job(
    partitions_def=weekly_partition,
    name="weekly_trip_update_job",
    selection=trips_by_week,
)
