CREATE OR REPLACE VIEW `cleaned_penguins_data.season_overview` AS
WITH team_base AS (
  SELECT
    season,
    season_label,
    team,
    games_played AS GP,
    wins AS W,
    losses AS L,
    ot_losses AS OL,
    points AS P,
    goals_for AS GF,
    goals_against AS GA,
    win_pct
  FROM `cleaned_penguins_data.regular_team_summary`
  WHERE team = 'PIT'
    AND season BETWEEN 20102011 AND 20192020
),

top_scorer AS (
  SELECT
    season,
    player_name AS top_point_scorer,
    points AS top_point_scorer_points
  FROM (
    SELECT
      season,
      player_name,
      points,
      goals,
      assists,
      ROW_NUMBER() OVER (
        PARTITION BY season
        ORDER BY points DESC, goals DESC, assists DESC, player_name
      ) AS rn
    FROM `cleaned_penguins_data.regular_player_leader`
    WHERE team = 'PIT'
      AND season BETWEEN 20102011 AND 20192020
      AND position_group <> 'GOALIE'
  )
  WHERE rn = 1
),

top_goalie AS (
  SELECT
    season,
    player_name AS top_goalie,
    wins AS top_goalie_wins
  FROM (
    SELECT
      season,
      player_name,
      wins,
      save_pct,
      gaa,
      ROW_NUMBER() OVER (
        PARTITION BY season
        ORDER BY wins DESC, save_pct DESC, gaa ASC, player_name
      ) AS rn
    FROM `cleaned_penguins_data.regular_player_leader`
    WHERE team = 'PIT'
      AND season BETWEEN 20102011 AND 20192020
      AND position_group = 'GOALIE'
  )
  WHERE rn = 1
),

playoff_base AS (
  SELECT
    season,
    playoff_finish
  FROM `cleaned_penguins_data.playoff_team_summary`
  WHERE team = 'PIT'
    AND season BETWEEN 20102011 AND 20192020
)

SELECT
  t.season,
  t.season_label,
  t.gp,
  t.w,
  t.l,
  t.ol,
  t.p,
  t.gf,
  t.ga,
  t.win_pct,

  s.top_point_scorer,
  s.top_point_scorer_points,
  CONCAT(s.top_point_scorer, ' (', CAST(s.top_point_scorer_points AS STRING), ')') AS top_scorer_display,

  g.top_goalie,
  g.top_goalie_wins,
  CONCAT(g.top_goalie, ' (', CAST(g.top_goalie_wins AS STRING), ')') AS top_goalie_display,

  p.playoff_finish

FROM team_base t
LEFT JOIN top_scorer s
  ON t.season = s.season
LEFT JOIN top_goalie g
  ON t.season = g.season
LEFT JOIN playoff_base p
  ON t.season = p.season
ORDER BY t.season;