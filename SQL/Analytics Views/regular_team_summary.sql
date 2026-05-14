CREATE OR REPLACE VIEW `cleaned_penguins_data.regular_team_summary` AS
WITH base AS (
  SELECT
    CAST(season AS INT64) AS season,
    team,
    game_type_code,
    CAST(team_goals AS INT64) AS goals_for,
    CAST(opp_goals  AS INT64) AS goals_against,
    result,             
    last_period_type,   
    CAST(is_home AS BOOL) AS is_home
  FROM `cleaned_penguins_data.v_games`
  WHERE game_type_code = 2   
),
agg AS (
  SELECT
    season,
    CONCAT(SUBSTR(CAST(season AS STRING), 1, 4),'-',SUBSTR(CAST(season AS STRING), 7, 2)) AS season_label,
    team,

    COUNT(*) AS games_played,

    SUM(CASE WHEN result = "W" THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = "L" THEN 1 ELSE 0 END) AS losses,

    SUM(CASE WHEN result = "L" AND last_period_type IN ("OT","SO") THEN 1 ELSE 0 END) AS ot_losses,
    SUM(CASE WHEN result = "L" AND last_period_type = "REG" THEN 1 ELSE 0 END) AS reg_losses,

    SUM(CASE WHEN result = "W" AND last_period_type = "REG" THEN 1 ELSE 0 END) AS reg_wins,
    SUM(CASE WHEN result = "W" AND last_period_type = "OT"  THEN 1 ELSE 0 END) AS ot_wins,
    SUM(CASE WHEN result = "W" AND last_period_type = "SO"  THEN 1 ELSE 0 END) AS so_wins,

    SUM(goals_for) AS goals_for,
    SUM(goals_against) AS goals_against,
    SUM(goals_for) - SUM(goals_against) AS goal_diff,

    SAFE_DIVIDE(SUM(CASE WHEN result = "W" THEN 1 ELSE 0 END), COUNT(*)) AS win_pct,

    
    (2 * SUM(CASE WHEN result = "W" THEN 1 ELSE 0 END)
     + 1 * SUM(CASE WHEN result = "L" AND last_period_type IN ("OT","SO") THEN 1 ELSE 0 END)) AS points
  FROM base
  GROUP BY season, team
)
SELECT *
FROM agg;