CREATE OR REPLACE VIEW `cleaned_penguins_data.opponent_record` AS
WITH base AS (
  SELECT
    CAST(season AS INT64) AS season,
    team,
    opponent,
    result,             
    last_period_type,  
    CAST(team_goals AS INT64) AS goals_for,
    CAST(opp_goals  AS INT64) AS goals_against
  FROM `cleaned_penguins_data.v_games`
  WHERE team = "PIT"
    AND game_type_code = 2   
)
SELECT
  opponent,
  COUNT(*) AS games_played,
  SUM(CASE WHEN result = "W" THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN result = "L" THEN 1 ELSE 0 END) AS losses,
  SUM(CASE WHEN result = "L" AND last_period_type IN ("OT","SO") THEN 1 ELSE 0 END) AS ot_losses,
  SUM(CASE WHEN result = "L" AND last_period_type = "REG" THEN 1 ELSE 0 END) AS reg_losses,
  SUM(goals_for) AS goals_for,
  SUM(goals_against) AS goals_against,
  SUM(goals_for) - SUM(goals_against) AS goal_diff,
  SAFE_DIVIDE(SUM(CASE WHEN result = "W" THEN 1 ELSE 0 END), COUNT(*)) AS win_pct,

  (2 * SUM(CASE WHEN result = "W" THEN 1 ELSE 0 END)
   + 1 * SUM(CASE WHEN result = "L" AND last_period_type IN ("OT","SO") THEN 1 ELSE 0 END)) AS points
FROM base
GROUP BY opponent;