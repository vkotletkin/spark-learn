-- Demo GSM/GPRS tables for Sedona Getis-Ord Gi*.
-- Init on first empty volume; re-apply anytime with clickhouse-client --multiquery.

CREATE DATABASE IF NOT EXISTS spark;

CREATE TABLE IF NOT EXISTS spark.cells
(
    cgi String,
    lac Int32,
    lon Float64,
    lat Float64,
    zone String,
    expected_imsi Int32
)
ENGINE = MergeTree
ORDER BY cgi;

CREATE TABLE IF NOT EXISTS spark.positions
(
    imsi String,
    ts DateTime64(3, 'UTC'),
    cgi String
)
ENGINE = MergeTree
ORDER BY (cgi, ts, imsi);

CREATE TABLE IF NOT EXISTS spark.connections
(
    imsi String,
    ts DateTime64(3, 'UTC'),
    conn_type String,
    peer String,
    duration_s Int32,
    bytes Int64
)
ENGINE = MergeTree
ORDER BY (imsi, ts);

TRUNCATE TABLE IF EXISTS spark.connections;
TRUNCATE TABLE IF EXISTS spark.positions;
TRUNCATE TABLE IF EXISTS spark.cells;

-- 10x10 grid of cell centroids (~1 km). Zones are built so Gi* != CH top:
--   stadium       — peak vs quieter neighbours → high Z
--   downtown      — uniformly busy → high count, Z ~ 0
--   suburb_spike  — modest count vs quiet ring → high Z
--   park          — hole → negative Z
INSERT INTO spark.cells
SELECT
    concat('CGI-', leftPad(toString(x), 2, '0'), '-', leftPad(toString(y), 2, '0')) AS cgi,
    1000 + y AS lac,
    37.50 + x * 0.015 AS lon,
    55.70 + y * 0.010 AS lat,
    multiIf(
        x = 2 AND y = 7, 'stadium',
        x BETWEEN 1 AND 3 AND y BETWEEN 6 AND 8, 'stadium_ring',
        x BETWEEN 6 AND 8 AND y BETWEEN 6 AND 8, 'downtown',
        x = 8 AND y = 1, 'suburb_spike',
        x <= 1 AND y <= 1, 'park',
        'quiet'
    ) AS zone,
    multiIf(
        x = 2 AND y = 7, 220,
        x BETWEEN 1 AND 3 AND y BETWEEN 6 AND 8, 45,
        x BETWEEN 6 AND 8 AND y BETWEEN 6 AND 8, 180,
        x = 8 AND y = 1, 90,
        x <= 1 AND y <= 1, 3,
        18
    ) AS expected_imsi
FROM
(
    SELECT
        toInt32(number % 10) AS x,
        toInt32(intDiv(number, 10)) AS y
    FROM numbers(100)
);

INSERT INTO spark.positions
SELECT
    concat('IMSI', replaceRegexpAll(cgi, '[^0-9]', ''), leftPad(toString(n), 4, '0')) AS imsi,
    toDateTime64('2026-09-21 12:00:00', 3, 'UTC') AS ts,
    cgi
FROM spark.cells
ARRAY JOIN range(1, expected_imsi + 1) AS n;

INSERT INTO spark.connections
SELECT
    imsi,
    ts,
    'GSM' AS conn_type,
    concat('B', right(imsi, 4)) AS peer,
    60 AS duration_s,
    0 AS bytes
FROM spark.positions
WHERE cityHash64(imsi) % 3 = 0;

INSERT INTO spark.connections
SELECT
    imsi,
    ts,
    'GPRS' AS conn_type,
    'internet' AS peer,
    0 AS duration_s,
    toInt64(1024 * (10 + cityHash64(imsi) % 100)) AS bytes
FROM spark.positions
WHERE cityHash64(imsi) % 5 = 0;
