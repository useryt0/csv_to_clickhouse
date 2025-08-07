CREATE TABLE cleaned_table AS
SELECT
    player_id,
    club_id,
    game_id,
    team_captain,
    COALESCE(position, '') AS position,
    CASE
        WHEN number IS NULL OR number = '' THEN NULL
        WHEN number ~ '^[0-9]+\D*$' THEN REGEXP_REPLACE(number, '\D', '', 'g')::INTEGER
        WHEN number !~ '^[0-9]' THEN NULL
        ELSE NULL
    END AS number,
    COALESCE(player_name, '') AS player_name,
    date::DATE,
    COALESCE(game_lineups_id, '') AS game_lineups_id,
    COALESCE(type, '') AS type
FROM game_lineups_table;