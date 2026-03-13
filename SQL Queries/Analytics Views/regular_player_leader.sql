CREATE OR REPLACE VIEW `cleaned_penguins_data.regular_player_leader` AS
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
    SAFE_CAST(goals_against AS INT64) AS goals_against,
    SAFE_CAST(save_pct AS FLOAT64) AS save_pct,
    SAFE_CAST(gaa AS FLOAT64) AS gaa,
    SAFE_CAST(points_per_game AS FLOAT64) AS points_per_game,
    SAFE_CAST(goals_per_game AS FLOAT64) AS goals_per_game,
    SAFE_CAST(shots_per_game AS FLOAT64) AS shots_per_game
  FROM `cleaned_penguins_data.v_season_player_stats`
  WHERE team = "PIT"
),
ranked AS (
  SELECT *,
    CONCAT(SUBSTR(CAST(season AS STRING), 1, 4),'-',SUBSTR(CAST(season AS STRING), 7, 2)) AS season_label,
    DENSE_RANK() OVER (PARTITION BY season ORDER BY points DESC, goals DESC, assists DESC) AS points_rank,
    DENSE_RANK() OVER (PARTITION BY season ORDER BY goals DESC, points DESC) AS goals_rank,
    DENSE_RANK() OVER (PARTITION BY season ORDER BY assists DESC, points DESC) AS assists_rank,
    DENSE_RANK() OVER (PARTITION BY season ORDER BY save_pct DESC, gaa ASC) AS goalie_rank
  FROM base
)
SELECT *
FROM ranked;