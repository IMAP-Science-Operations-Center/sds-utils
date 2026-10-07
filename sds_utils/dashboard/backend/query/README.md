### Data ingestion

Create or update the dashboard's database of runs and metadata with the following command, adjusting start and end dates as you see fit.

Note that it takes some time for the run metadata to be queried and processed.

The database stores the earliest and latest dates ingested (using the time of processing if the end date is in the future), and only date ranges not yet ingested are queried, processed, and saved in the database.

```
poetry run python -m sds_utils.dashboard.backend.query.run_ingestion --start-date 20260925 --end-date 20261231
```
