from google.cloud import bigquery
from google.oauth2 import service_account
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

credentials = service_account.Credentials.from_service_account_file(
    os.path.join(BASE_DIR, "credentials", "service_account.json")
)

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

print("Connected successfully!\n")

output_dir = os.path.join(BASE_DIR, "penguins-dashboard", "src", "data")
os.makedirs(output_dir, exist_ok=True)

views = {
    "season_overview": "SELECT * FROM `nhl-data-engineering-488520.cleaned_penguins_data.season_overview`",
    "regular_player_leader": "SELECT * FROM `nhl-data-engineering-488520.cleaned_penguins_data.regular_player_leader`",
    "regular_player_decade_leader": "SELECT * FROM `nhl-data-engineering-488520.cleaned_penguins_data.regular_player_decade_leader`",
    "playoff_team_summary": "SELECT * FROM `nhl-data-engineering-488520.cleaned_penguins_data.playoff_team_summary`",
    "playoff_opponent_summary": "SELECT * FROM `nhl-data-engineering-488520.cleaned_penguins_data.playoff_opponent_summary`",
    "opponent_record": "SELECT * FROM `nhl-data-engineering-488520.cleaned_penguins_data.opponent_record`",
    "home_away_summary": "SELECT * FROM `nhl-data-engineering-488520.cleaned_penguins_data.home_away_summary`",
    "monthly_summary": "SELECT * FROM `nhl-data-engineering-488520.cleaned_penguins_data.monthly_summary`",
}

for name, query in views.items():
    print(f"Exporting {name}...")
    df = client.query(query).to_dataframe()
    df.to_json(f"{output_dir}/{name}.json", orient="records", indent=2)
    print(f"  -> saved {len(df)} rows")

print("\nAll done!")