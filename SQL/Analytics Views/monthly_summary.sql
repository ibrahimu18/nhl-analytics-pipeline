CREATE OR REPLACE VIEW `cleaned_penguins_data.monthly_summary` AS
WITH base AS (
  SELECT
    CAST(season AS INT64) AS season,
    team,
    game_year_month,          
    result,
    last_period_type,
    CAST(team_goals AS INT64) AS goals_for,
    CAST(opp_goals  AS INT64) AS goals_against
  FROM `cleaned_penguins_data.v_games`
  WHERE team = "PIT"
    AND game_type_code = 2)

SELECT
  season,
  CONCAT(SUBSTR(CAST(season AS STRING), 1, 4),'-',SUBSTR(CAST(season AS STRING), 7, 2)) AS season_label,
  team,
  game_year_month,
  COUNT(*) AS games_played,
  SUM(CASE WHEN result = "W" THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN result = "L" THEN 1 ELSE 0 END) AS losses,
  SUM(CASE WHEN result = "L" AND last_period_type IN ("OT","SO") THEN 1 ELSE 0 END) AS ot_losses,
  (2 * SUM(CASE WHEN result = "W" THEN 1 ELSE 0 END)
   + 1 * SUM(CASE WHEN result = "L" AND last_period_type IN ("OT","SO") THEN 1 ELSE 0 END)) AS points,
  SUM(goals_for) AS goals_for,
  SUM(goals_against) AS goals_against,
  SUM(goals_for) - SUM(goals_against) AS goal_diff,
  SAFE_DIVIDE(SUM(goals_for), COUNT(*)) AS goals_for_per_game,
  SAFE_DIVIDE(SUM(goals_against), COUNT(*)) AS goals_against_per_game
FROM base
GROUP BY season, team, game_year_month
ORDER BY season, game_year_month;