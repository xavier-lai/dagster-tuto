create_request_json_file:
	@echo "Creating request.json file"
	@mkdir -p dagster_university/data/requests/
	@echo '{ "start_date": "2023-01-10", "end_date": "2023-01-25", "borough": "Staten Island" }' > dagster_university/data/requests/$(file_name).json