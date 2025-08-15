CREATE TABLE {TARGET_DB}.{TARGET_TABLE}
(
    `player_id` UInt32,
    `club_id` UInt32,
    `game_id` UInt32,
    `team_captain` UInt8,
    `position` LowCardinality(String),
    `number` Nullable(UInt32),
    `player_name` String,
    `date` Date,
    `game_lineups_id` UUID,
    `type` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (date, game_id, player_id)
SETTINGS index_granularity = 8192