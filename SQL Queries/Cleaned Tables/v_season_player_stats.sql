CREATE OR REPLACE VIEW `nhl-data-engineering-488520.cleaned_penguins_data.v_season_player_stats` AS
SELECT
  season,
  team,
  franchise_id,
  player_id,
  player_name,
  position_group,

  SAFE_CAST(SUBSTR(season, 1, 4) AS INT64) AS season_start_year,
  SAFE_CAST(SUBSTR(season, 5, 4) AS INT64) AS season_end_year,

  games_played,
  goals,
  assists,
  points,
  plus_minus,
  penalty_minutes,
  shots,

  wins,
  losses,
  ot_losses,
  saves,
  shots_against,
  goals_against,
  save_pct,
  gaa,

  -- useful derived metrics
  SAFE_DIVIDE(points, games_played) AS points_per_game,
  SAFE_DIVIDE(goals, games_played) AS goals_per_game,
  SAFE_DIVIDE(shots, games_played) AS shots_per_game

FROM `nhl-data-engineering-488520.raw_penguins_data.season_player_stats`
WHERE season IS NOT NULL
  AND team IS NOT NULL
  AND player_name IS NOT NULL;