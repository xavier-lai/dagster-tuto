from dagster import AssetSelection, define_asset_job

trips_by_week = AssetSelection.assets("trips_by_week")
trip_update_job = define_asset_job(
    name="trip_update_job", selection=AssetSelection.all() - trips_by_week
)
weekly_updated_trip_job = define_asset_job(
    name="weekly_update_job", selection=trips_by_week
)
