# sds-utils

## scrubber

Setup:  `poetry install`

Run: `poetry run python -m sds_utils.scrubber`


## Dagster run status dashboard

### Setup

* Install locally: `poetry install`
* Set environment variables:
  * `DAGSTER_BASE_URL="https://processing.imap-mission.com"`
  * `DAGSTER_API_KEY=<API-KEY>`
  * `DASHBOARD_APP_USERNAME=<username-of-your-choice>`
  * `DASHBOARD_APP_DB_URL=<AWS-db-url>`
  * If desired, set `DASHBOARD_APP_DARK_MODE=1` to enable dark mode
    * It can alternatively be set in the dashboard's settings GUI
    * Either option is persisted across sessions

### Run the dashboard

```
poetry run python -m sds_utils.dashboard.frontend.app
```
