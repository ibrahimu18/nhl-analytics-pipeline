CREATE OR REPLACE VIEW `cleaned_penguins_data.playoff_opponent_summary` AS
WITH playoff AS (
  SELECT
    CAST(season AS INT64) AS season,
    opponent,
    result,
    CAST(team_goals AS INT64) AS goals_for,
    CAST(opp_goals  AS INT64) AS goals_against
  FROM `cleaned_penguins_data.v_games`
  WHERE team = 'PIT'
    AND game_type_code = 3
),

series_by_season AS (
  SELECT
    season,
    opponent,
    COUNT(*) AS games_in_series,
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS pit_wins,
    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS pit_losses
  FROM playoff
  GROUP BY season, opponent
),

series_outcomes AS (
  SELECT
    season,
    opponent,
    games_in_series,
    pit_wins,
    pit_losses,

    CASE
      WHEN season = 20192020 AND games_in_series <= 5 THEN 3
      ELSE 4
    END AS win_threshold
  FROM series_by_season
),

series_labeled AS (
  SELECT
    season,
    opponent,
    games_in_series,
    pit_wins,
    pit_losses,
    win_threshold,
    CASE WHEN pit_wins  >= win_threshold THEN 1 ELSE 0 END AS series_win,
    CASE WHEN pit_losses >= win_threshold THEN 1 ELSE 0 END AS series_loss,
    CASE
      WHEN pit_wins  >= win_threshold THEN 'W'
      WHEN pit_losses >= win_threshold THEN 'L'
      ELSE 'INCOMPLETE_OR_BAD_DATA'
    END AS series_result
  FROM series_outcomes
),

opponent_games AS (
  SELECT
    opponent,
    COUNT(*) AS games_played,
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
    SUM(goals_for) AS goals_for,
    SUM(goals_against) AS goals_against
  FROM playoff
  GROUP BY opponent
),

opponent_series AS (
  SELECT
    opponent,
    COUNT(*) AS series_played,
    SUM(series_win)  AS series_wins,
    SUM(series_loss) AS series_losses,
    SUM(CASE WHEN series_result = 'INCOMPLETE_OR_BAD_DATA' THEN 1 ELSE 0 END) AS flagged_series
  FROM series_labeled
  GROUP BY opponent
)

SELECT
  g.opponent,
  s.series_played,
  s.series_wins,
  s.series_losses,
  CONCAT(CAST(s.series_wins AS STRING), '-', CAST(s.series_losses AS STRING)) AS series_head_to_head,
  s.flagged_series,
  g.games_played,
  g.wins,
  g.losses,
  g.goals_for,
  g.goals_against,
  (g.goals_for - g.goals_against) AS goal_diff
FROM opponent_games g
JOIN opponent_series s USING (opponent)
ORDER BY s.series_wins DESC, s.series_losses ASC, goal_diff DESC;