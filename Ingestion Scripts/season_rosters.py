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
TABLE_ID = "season_rosters"

# Configuration
TEAM = "PIT"
SEASONS = ["20102011", "20112012", "20122013", "20132014", "20142015", "20152016", "20162017", "20172018", "20182019", "20192020",]

# Set Up
client = NHLClient()
bq = bigquery.Client(credentials=credentials, project=PROJECT_ID)

table_fqn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Create Table
schema = [bigquery.SchemaField("season", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("team", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("player_name", "STRING", mode="NULLABLE"),
          bigquery.SchemaField("position", "STRING", mode="NULLABLE"),
          bigquery.SchemaField("jersey_number", "INTEGER", mode="NULLABLE"),
          bigquery.SchemaField("roster_key", "STRING", mode="REQUIRED"),]

try:
    bq.create_table(bigquery.Table(table_fqn, schema=schema))
    print(f"Created table: {table_fqn}")
except Exception:
    print(f"Table already exists: {table_fqn}")

def safe_int(x):
    try:
        return int(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

def extract_players(roster_payload):
    """Handle common roster payload shapes from nhlpy."""
    if isinstance(roster_payload, list):
        return roster_payload

    if isinstance(roster_payload, dict):
        if "roster" in roster_payload and isinstance(roster_payload["roster"], list):
            return roster_payload["roster"]

        players = []
        for k in ["forwards", "defensemen", "goalies", "skaters"]:
            if k in roster_payload and isinstance(roster_payload[k], list):
                players.extend(roster_payload[k])
        return players

    return []

# Load data for each season
total_loaded = 0

for season in SEASONS:
    roster_payload = client.teams.team_roster(team_abbr=TEAM, season=season)
    players = extract_players(roster_payload)

    if not players:
        raise ValueError(f"No players found in roster payload for season {season}")

    rows = []
    for p in players:
        player_id = str(p.get("id") or p.get("playerId") or p.get("player_id") or "").strip()
        if not player_id:
            continue

        name = (
            p.get("fullName")
            or p.get("name")
            or (f"{p.get('firstName','')} {p.get('lastName','')}".strip())
            or None
        )

        position = p.get("positionCode") or p.get("position") or None
        jersey = safe_int(p.get("sweaterNumber") or p.get("jerseyNumber") or p.get("jersey_number"))

        rows.append({"season": season,
                     "team": TEAM,
                     "player_id": player_id,
                     "player_name": name,
                     "position": position,
                     "jersey_number": jersey,
                     "roster_key": f"{season}-{TEAM}-{player_id}",})

    job_config = bigquery.LoadJobConfig(schema=schema,write_disposition="WRITE_APPEND",)
    load_job = bq.load_table_from_json(rows, table_fqn, job_config=job_config)
    load_job.result()

    total_loaded += len(rows)
    print(f"Season {season}: loaded {len(rows)} roster rows")

print(f"Loaded {total_loaded} total rows into {table_fqn}")