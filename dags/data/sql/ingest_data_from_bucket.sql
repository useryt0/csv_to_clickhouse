-- INSERT INTO {TARGET_DB}.{TARGET_TABLE}
INSERT INTO {{ params.TARGET_DB }}.{{ params.TARGET_TABLE }}
SELECT
    toUInt32(player_id) AS player_id,
    toUInt32(club_id) AS club_id,
    toUInt32(game_id) AS game_id,
    toUInt8(team_captain) AS team_captain,
    ifNull(position, '') AS position,
    multiIf(
        (number = '' OR number IS NULL), NULL,
        match(number, '^[0-9]+\\D*$'), toUInt32(replaceRegexpAll(number, '\\D', '')),
        match(number, '^[0-9]'), NULL,
        NULL
    ) AS number,
    ifNull(player_name, '') AS player_name,
    toDate(date) AS date,
    toUUID(game_lineups_id) AS game_lineups_id,
    ifNull(type, '') AS type
FROM s3(
    '{{ params.minio_url }}',
    'minio',
    'minio123',
    'CSVWithNames'
);
