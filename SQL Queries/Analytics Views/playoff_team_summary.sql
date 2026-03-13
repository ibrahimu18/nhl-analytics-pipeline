CREATE OR REPLACE VIEW `cleaned_penguins_data.playoff_team_summary` AS
WITH playoff AS (
  SELECT
    CAST(season AS INT64) AS season,
    team,
    opponent,
    result
  FROM `cleaned_penguins_data.v_games`
  WHERE team = 'PIT'
    AND game_type_code = 3
),

series_by_season AS (
  SELECT
    season,
    team,
    opponent,
    COUNT(*) AS games_in_series,
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS pit_wins,
    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS pit_losses
  FROM playoff
  GROUP BY season, team, opponent
),

series_labeled AS (
  SELECT
    season,
    team,
    opponent,
    games_in_series,
    pit_wins,
    pit_losses,
    CASE
      WHEN season = 20192020 AND games_in_series <= 5 THEN 3
      ELSE 4
    END AS win_threshold,
    CASE
      WHEN pit_wins >= CASE WHEN season = 20192020 AND games_in_series <= 5 THEN 3 ELSE 4 END THEN 1
      ELSE 0
    END AS series_win,
    CASE
      WHEN pit_losses >= CASE WHEN season = 20192020 AND games_in_series <= 5 THEN 3 ELSE 4 END THEN 1
      ELSE 0
    END AS series_loss
  FROM series_by_season
),

season_totals AS (
  SELECT
    season,
    team,
    COUNT(*) AS playoff_series,                 
    SUM(series_win)  AS series_wins,
    SUM(series_loss) AS series_losses,
    SUM(pit_wins)   AS playoff_wins,
    SUM(pit_losses) AS playoff_losses,
    SUM(games_in_series) AS playoff_games,
    SUM(CASE WHEN series_win = 0 AND series_loss = 0 THEN 1 ELSE 0 END) AS flagged_series
  FROM series_labeled
  GROUP BY season, team)

SELECT
  season,
  team,
  playoff_games,
  playoff_wins,
  playoff_losses,
  playoff_series,
  series_wins,
  series_losses,
  flagged_series,

  CASE
    WHEN playoff_games IS NULL OR playoff_games = 0 THEN 'Missed playoffs'
    WHEN season = 20192020 AND series_wins = 0 AND series_losses = 1 AND playoff_series = 1
      THEN 'Lost in Qualifiers'
    WHEN series_wins = 0 AND series_losses = 1 THEN 'Lost in First round'
    WHEN series_wins = 1 AND series_losses = 1 THEN 'Lost in Second round'
    WHEN series_wins = 2 AND series_losses = 1 THEN 'Lost in Conference Finals'
    WHEN series_wins = 3 AND series_losses = 1 THEN 'Lost in Stanley Cup Finals'
    WHEN series_wins = 4 AND series_losses = 0 THEN 'Won Stanley Cup'
    ELSE 'Unknown'
  END AS playoff_finish
FROM season_totals
ORDER BY season;