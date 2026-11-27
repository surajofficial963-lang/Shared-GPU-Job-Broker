CREATE DATABASE IF NOT EXISTS broker;
CREATE TABLE IF NOT EXISTS broker.history
(
    kind LowCardinality(String),
    id String,
    team LowCardinality(String),
    started Nullable(Int64),
    ended Int64,
    payload String,
    INDEX history_id id TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(toDateTime(ended, 'UTC'))
ORDER BY (kind, ended, id);
