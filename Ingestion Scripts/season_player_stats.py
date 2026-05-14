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
TABLE_ID = "season_player_stats"

# Configuration
TEAM = "PIT"
FRANCHISE_ID = "17"
SEASONS = ["20102011", "20112012", "20122013", "20132014", "20142015", "20152016", "20162017", "20172018", "20182019", "20192020",]

# Set Up
client = NHLClient()
bq = bigquery.Client(credentials=credentials, project=PROJECT_ID)
table_fqn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Create Table
schema = [bigquery.SchemaField("season", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("team", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("franchise_id", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("player_id", "STRING"),
          bigquery.SchemaField("player_name", "STRING"),
          bigquery.SchemaField("position_group", "STRING"),
          
          bigquery.SchemaField("games_played", "INTEGER"),
          bigquery.SchemaField("goals", "INTEGER"),
          bigquery.SchemaField("assists", "INTEGER"),
          bigquery.SchemaField("points", "INTEGER"),
          bigquery.SchemaField("plus_minus", "INTEGER"),
          bigquery.SchemaField("penalty_minutes", "INTEGER"),
          bigquery.SchemaField("shots", "INTEGER"),
          
          bigquery.SchemaField("wins", "INTEGER"),
          bigquery.SchemaField("losses", "INTEGER"),
          bigquery.SchemaField("ot_losses", "INTEGER"),
          bigquery.SchemaField("saves", "INTEGER"),
          bigquery.SchemaField("shots_against", "INTEGER"),
          bigquery.SchemaField("goals_against", "INTEGER"),
          bigquery.SchemaField("save_pct", "FLOAT"),
          bigquery.SchemaField("gaa", "FLOAT"),]

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

def safe_float(x):
    try:
        return float(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

total_loaded = 0

for season in SEASONS:
    print(f"\nProcessing season {season}")

    rows = []
    skaters = client.stats.skater_stats_summary(start_season=season,end_season=season,franchise_id=FRANCHISE_ID)
    skater_list = skaters if isinstance(skaters, list) else skaters.get("data") or skaters.get("rows") or []

    print(f"  Skaters: {len(skater_list)}")

    for p in skater_list:
        rows.append({"season": season,
                     "team": TEAM,
                     "franchise_id": FRANCHISE_ID,
                     "player_id": str(p.get("playerId") or p.get("id") or "") or None,
                     "player_name": p.get("skaterFullName") or p.get("playerName") or p.get("fullName"),
                     "position_group": "SKATER",
                     "games_played": safe_int(p.get("gamesPlayed") or p.get("games")),
                     "goals": safe_int(p.get("goals")),
                     "assists": safe_int(p.get("assists")),
                     "points": safe_int(p.get("points")),
                     "plus_minus": safe_int(p.get("plusMinus")),
                     "penalty_minutes": safe_int(p.get("penaltyMinutes") or p.get("pim")),
                     "shots": safe_int(p.get("shots")),
                     "wins": None,
                     "losses": None,
                     "ot_losses": None,
                     "saves": None,
                     "shots_against": None,
                     "goals_against": None,
                     "save_pct": None,
                     "gaa": None,})

   
    goalies = client.stats.goalie_stats_summary(start_season=season,end_season=season,franchise_id=FRANCHISE_ID)
    goalie_list = goalies if isinstance(goalies, list) else goalies.get("data") or goalies.get("rows") or []

    print(f"  Goalies: {len(goalie_list)}")

    for g in goalie_list:
        rows.append({"season": season,
                     "team": TEAM,
                     "franchise_id": FRANCHISE_ID,
                     "player_id": str(g.get("playerId") or g.get("id") or "") or None,
                     "player_name": g.get("goalieFullName") or g.get("playerName") or g.get("fullName"),
                     "position_group": "GOALIE",
                     "games_played": safe_int(g.get("gamesPlayed") or g.get("games")),
                     "goals": None,
                     "assists": None,
                     "points": None,
                     "plus_minus": None,
                     "penalty_minutes": None,
                     "shots": None,
                     "wins": safe_int(g.get("wins")),
                     "losses": safe_int(g.get("losses")),
                     "ot_losses": safe_int(g.get("otLosses")),
                     "saves": safe_int(g.get("saves")),
                     "shots_against": safe_int(g.get("shotsAgainst")),
                     "goals_against": safe_int(g.get("goalsAgainst")),
                     "save_pct": safe_float(g.get("savePct") or g.get("savePercentage")),
                     "gaa": safe_float(g.get("goalsAgainstAverage") or g.get("gaa")),})

    job_config = bigquery.LoadJobConfig(schema=schema,write_disposition="WRITE_APPEND",)
    load_job = bq.load_table_from_json(rows, table_fqn, job_config=job_config)
    load_job.result()

    total_loaded += len(rows)
    print(f"Loaded {len(rows)} rows")

print(f"Loaded {total_loaded} total rows into {table_fqn}")