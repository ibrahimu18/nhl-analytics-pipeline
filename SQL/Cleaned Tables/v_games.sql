CREATE OR REPLACE VIEW `cleaned_penguins_data.v_games` AS
WITH b AS (
  SELECT
    game_id,

    SAFE_CAST(JSON_VALUE(boxscore_json, '$.gameType') AS INT64) AS game_type_code,
    JSON_VALUE(boxscore_json, '$.gameOutcome.lastPeriodType') AS last_period_type,

    JSON_VALUE(boxscore_json, '$.homeTeam.abbrev') AS home_abbrev,
    JSON_VALUE(boxscore_json, '$.awayTeam.abbrev') AS away_abbrev,

    SAFE_CAST(JSON_VALUE(boxscore_json, '$.homeTeam.score') AS INT64) AS home_score,
    SAFE_CAST(JSON_VALUE(boxscore_json, '$.awayTeam.score') AS INT64) AS away_score,

    JSON_VALUE(boxscore_json, '$.venue.default') AS venue,
    JSON_VALUE(boxscore_json, '$.venueLocation.default') AS venue_location,

    -- fallback time (string -> timestamp)
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(boxscore_json, '$.startTimeUTC')) AS start_time_utc_from_box
  FROM `nhl-data-engineering-488520.raw_penguins_data.game_boxscores`
)

SELECT
  m.game_id,
  m.season,
  m.team,
  m.game_date,

  -- Prefer metadata start time; fallback to boxscore time
  COALESCE(m.start_time_utc, b.start_time_utc_from_box) AS start_time_utc,

  -- Season breakdown (e.g., "20102011" -> 2010, 2011)
  SAFE_CAST(SUBSTR(m.season, 1, 4) AS INT64) AS season_start_year,
  SAFE_CAST(SUBSTR(m.season, 5, 4) AS INT64) AS season_end_year,

  -- Simple date dims
  FORMAT_DATE('%Y-%m', m.game_date) AS game_year_month,
  FORMAT_DATE('%a', m.game_date) AS game_dayofweek,
  CASE WHEN EXTRACT(DAYOFWEEK FROM m.game_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,

  -- Game type
  b.game_type_code,
  CASE b.game_type_code
    WHEN 1 THEN 'PRESEASON'
    WHEN 2 THEN 'REGULAR'
    WHEN 3 THEN 'PLAYOFF'
    ELSE 'OTHER'
  END AS game_type,

  -- Home/away + opponent
  b.home_abbrev,
  b.away_abbrev,
  CASE WHEN b.home_abbrev = m.team THEN TRUE ELSE FALSE END AS is_home,
  CASE WHEN b.home_abbrev = m.team THEN b.away_abbrev ELSE b.home_abbrev END AS opponent,

  -- Team-perspective scoring
  b.home_score,
  b.away_score,
  CASE WHEN b.home_abbrev = m.team THEN b.home_score ELSE b.away_score END AS team_goals,
  CASE WHEN b.home_abbrev = m.team THEN b.away_score ELSE b.home_score END AS opp_goals,

  CASE
    WHEN (CASE WHEN b.home_abbrev = m.team THEN b.home_score ELSE b.away_score END)
       > (CASE WHEN b.home_abbrev = m.team THEN b.away_score ELSE b.home_score END)
    THEN 'W'
    ELSE 'L'
  END AS result,

  b.last_period_type,
  b.venue,
  b.venue_location

FROM `nhl-data-engineering-488520.raw_penguins_data.games_metadata` m
LEFT JOIN b
  ON b.game_id = m.game_id;