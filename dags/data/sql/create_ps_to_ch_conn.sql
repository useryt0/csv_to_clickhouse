CREATE TABLE {TARGET_DB}.{CLICKHOUSE_POSTGRES_VIEW}
(
    `player_id` UInt32,
    `club_id` UInt32,
    `game_id` UInt32,
    `team_captain` UInt8,
    `position` String,
    `number` Nullable(UInt32),
    `player_name` String,
    `date` Date,
    `game_lineups_id` UUID,
    `type` String
)
ENGINE = PostgreSQL('postgres:5432', 'dag_database', 'cleaned_table', 'airflow', 'airflow')