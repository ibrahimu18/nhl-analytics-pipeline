import os
import time
from nhlpy import NHLClient
from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not CREDENTIALS_PATH: raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not set.")
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

PROJECT_ID = credentials.project_id
DATASET_ID = "raw_penguins_data"
TABLE_ID = "games_metadata"

# Configuration
TEAM = "PIT"
SEASONS = ["20102011", "20112012", "20122013", "20132014", "20142015", "20152016", "20162017", "20172018", "20182019", "20192020",]

# Setup
nhl = NHLClient()
bq = bigquery.Client(credentials=credentials, project=PROJECT_ID)

table_fqn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Create table in BigQuery
schema = [bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("season", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("team", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("game_date", "DATE", mode="REQUIRED"),
          bigquery.SchemaField("start_time_utc", "TIMESTAMP", mode="REQUIRED"),]

try:
    bq.create_table(bigquery.Table(table_fqn, schema=schema))
    print(f"Created table: {table_fqn}")
except Exception:
    print(f"Table already exists: {table_fqn}")

# Load data for each season
total_rows = 0

for season in SEASONS:
    schedule = nhl.schedule.team_season_schedule(team_abbr=TEAM, season=season)
    games = schedule["games"]
    print(f"\nSeason {season}: pulled {len(games)} games")

    rows = []
    for g in games:
        game_id = str(g["id"])
        start_time = g["startTimeUTC"]      
        game_date = start_time[:10]         

        rows.append({"game_id": game_id,
                     "season": season,
                     "team": TEAM,
                     "game_date": game_date,
                     "start_time_utc": start_time,})

    job_config = bigquery.LoadJobConfig(schema=schema,write_disposition="WRITE_APPEND",)
    load_job = bq.load_table_from_json(rows, table_fqn, job_config=job_config)
    load_job.result()

    total_rows += len(rows)
    print(f"Loaded {len(rows)} rows for {season}")

print(f"Loaded {total_rows} total rows into {table_fqn}")