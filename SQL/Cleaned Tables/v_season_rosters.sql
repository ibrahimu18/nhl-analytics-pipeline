CREATE OR REPLACE VIEW `nhl-data-engineering-488520.cleaned_penguins_data.v_season_rosters` AS
SELECT
  r.*,

  CASE
    -- If it looks like "{'default': 'Mike'} {'default': 'Comrie'}"
    WHEN REGEXP_CONTAINS(player_name, r"\{\s*'default'\s*:\s*'[^']+'\s*\}")
    THEN TRIM(
      CONCAT(
        COALESCE(REGEXP_EXTRACT(player_name, r"\{\s*'default'\s*:\s*'([^']+)'\s*\}"), ''),
        ' ',
        COALESCE(REGEXP_EXTRACT(player_name, r"\}\s*\{\s*'default'\s*:\s*'([^']+)'\s*\}"), '')
      )
    )
    ELSE player_name
  END AS player_name_clean

FROM `nhl-data-engineering-488520.raw_penguins_data.season_rosters` r;