CREATE OR REPLACE VIEW `nhl-data-engineering-488520.cleaned_penguins_data.v_games_metadata` AS
SELECT
  game_id,
  team,
  season,

  SAFE_CAST(SUBSTR(season, 1, 4) AS INT64) AS season_start_year,
  SAFE_CAST(SUBSTR(season, 5, 4) AS INT64) AS season_end_year,

  game_date,
  start_time_utc,

  DATETIME(start_time_utc) AS start_datetime_utc,
  DATETIME(start_time_utc, "America/Toronto") AS start_datetime_et,

  EXTRACT(YEAR FROM game_date) AS game_year,
  EXTRACT(MONTH FROM game_date) AS game_month,
  FORMAT_DATE('%Y-%m', game_date) AS game_year_month,

  EXTRACT(DAYOFWEEK FROM game_date) AS game_dayofweek_num,  
  FORMAT_DATE('%a', game_date) AS game_dayofweek,
  CASE WHEN EXTRACT(DAYOFWEEK FROM game_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend

FROM `nhl-data-engineering-488520.raw_penguins_data.games_metadata`;