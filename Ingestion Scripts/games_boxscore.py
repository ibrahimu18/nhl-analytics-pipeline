from nhlpy import NHLClient
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
import time

CREDENTIALS_PATH = "/Users/ibrahim/Downloads/NHL Data Engineering/credentials/service_account.json"
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
PROJECT_ID = credentials.project_id
DATASET_ID = "raw_penguins_data"
TABLE_ID = "game_boxscores"

# Configuration
TEAM = "PIT"
SEASONS = ["20102011", "20112012", "20122013", "20132014", "20142015",
           "20152016", "20162017", "20172018", "20182019", "20192020",]

SLEEP = 0.2
BATCH_SIZE = 10

# Setup
nhl = NHLClient()
bq = bigquery.Client(credentials=credentials, project=PROJECT_ID)

table_fqn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Create table in Biguery
schema = [bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("season", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("team", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("boxscore_json", "JSON", mode="REQUIRED"),]

bq.create_table(bigquery.Table(table_fqn, schema=schema))
print(f"Created table: {table_fqn}")

def load_batch(batch_rows: list[dict]):
    if not batch_rows:
        return
    job_config = bigquery.LoadJobConfig(schema=schema,write_disposition="WRITE_APPEND",)
    job = bq.load_table_from_json(batch_rows, table_fqn, job_config=job_config)
    job.result()

# Load data for each season
total_loaded = 0

for season in SEASONS:
    schedule = nhl.schedule.team_season_schedule(team_abbr=TEAM, season=season)
    games = schedule["games"]
    print(f"\nSeason {season}: {len(games)} games")

    batch = []
    season_loaded = 0

    for idx, g in enumerate(games, start=1):
        game_id = str(g["id"])
        boxscore = nhl.game_center.boxscore(game_id=game_id)
        batch.append({"game_id": game_id,"season": season,"team": TEAM,"boxscore_json": boxscore,})

        if SLEEP: time.sleep(SLEEP)

        if len(batch) >= BATCH_SIZE:
            load_batch(batch)
            total_loaded += len(batch)
            season_loaded += len(batch)
            print(f"Loaded batch ({season_loaded}/{len(games)})")
            batch = []
            
    if batch:
        load_batch(batch)
        total_loaded += len(batch)
        season_loaded += len(batch)
        print(f"Loaded final batch ({season_loaded}/{len(games)})")

    print(f"Season {season} complete: loaded {season_loaded} boxscores")

print(f"Loaded {total_loaded} total rows into {table_fqn}")