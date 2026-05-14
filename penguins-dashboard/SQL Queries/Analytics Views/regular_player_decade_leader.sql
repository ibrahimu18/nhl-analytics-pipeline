CREATE OR REPLACE VIEW `cleaned_penguins_data.regular_player_decade_leader` AS
WITH base AS (
  SELECT
    CAST(season AS INT64) AS season,
    team,
    CAST(franchise_id AS INT64) AS franchise_id,
    CAST(player_id AS INT64) AS player_id,
    player_name,
    position_group,
    CAST(games_played AS INT64) AS games_played,
    CAST(goals AS INT64) AS goals,
    CAST(assists AS INT64) AS assists,
    CAST(points AS INT64) AS points,
    SAFE_CAST(plus_minus AS INT64) AS plus_minus,
    SAFE_CAST(penalty_minutes AS INT64) AS penalty_minutes,
    SAFE_CAST(shots AS INT64) AS shots,
    SAFE_CAST(wins AS INT64) AS wins,
    SAFE_CAST(losses AS INT64) AS losses,
    SAFE_CAST(ot_losses AS INT64) AS ot_losses,
    SAFE_CAST(saves AS INT64) AS saves,
    SAFE_CAST(shots_against AS INT64) AS shots_against,
    SAFE_CAST(goals_against AS INT64) AS goals_against
  FROM `cleaned_penguins_data.v_season_player_stats`
  WHERE team = "PIT"
    AND CAST(season AS INT64) BETWEEN 20102011 AND 20192020
),
decade AS (
  SELECT
    "2010-11 to 2019-20" AS decade_label,
    team,
    franchise_id,
    player_id,
    ANY_VALUE(player_name) AS player_name,
    ANY_VALUE(position_group) AS position_group,
    SUM(games_played) AS games_played,
    SUM(goals) AS goals,
    SUM(assists) AS assists,
    SUM(points) AS points,
    SUM(plus_minus) AS plus_minus,
    SUM(penalty_minutes) AS penalty_minutes,
    SUM(shots) AS shots,
    SUM(wins) AS wins,
    SUM(losses) AS losses,
    SUM(ot_losses) AS ot_losses,
    SUM(saves) AS saves,
    SUM(shots_against) AS shots_against,
    SUM(goals_against) AS goals_against,
    SAFE_DIVIDE(SUM(points), NULLIF(SUM(games_played), 0)) AS points_per_game,
    SAFE_DIVIDE(SUM(goals), NULLIF(SUM(games_played), 0)) AS goals_per_game,
    SAFE_DIVIDE(SUM(shots), NULLIF(SUM(games_played), 0)) AS shots_per_game,
    SAFE_DIVIDE(SUM(saves), NULLIF(SUM(shots_against), 0)) AS save_pct,
    SAFE_DIVIDE(SUM(goals_against), NULLIF(SUM(games_played), 0)) AS gaa

  FROM base
  GROUP BY
    team, franchise_id, player_id
),
ranked AS (
  SELECT*,
    DENSE_RANK() OVER (ORDER BY points DESC, goals DESC, assists DESC) AS points_rank,
    DENSE_RANK() OVER (ORDER BY goals DESC, points DESC) AS goals_rank,
    DENSE_RANK() OVER (ORDER BY assists DESC, points DESC) AS assists_rank,
    DENSE_RANK() OVER (ORDER BY save_pct DESC, gaa ASC) AS goalie_rank
  FROM decade
)
SELECT *
FROM ranked;