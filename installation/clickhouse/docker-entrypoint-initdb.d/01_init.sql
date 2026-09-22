-- Demo base stations and GPRS facts for Sedona Getis-Ord Gi*.
-- Init on first empty volume; re-apply anytime with clickhouse-client --multiquery.

CREATE DATABASE IF NOT EXISTS spark;

DROP VIEW IF EXISTS spark.bs_load;
DROP TABLE IF EXISTS spark.connections;
DROP TABLE IF EXISTS spark.positions;
DROP TABLE IF EXISTS spark.cells;
DROP TABLE IF EXISTS spark.gprs_data;
DROP TABLE IF EXISTS spark.base_stations;

CREATE TABLE spark.base_stations
(
    id Int32,
    lat Float64,
    lon Float64,
    code Int32,
    lac Int32,
    cellid Int32
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE spark.gprs_data
(
    timestamp DateTime64(3, 'UTC'),
    identificator String,
    code Int32,
    lac Int32,
    cellid Int32
)
ENGINE = MergeTree
ORDER BY (code, lac, cellid, timestamp, identificator);

-- 100k stations inside Moscow (approx. MKAD box).
-- GPRS volume is uneven so Gi* != raw top:
--   Luzhniki      — peak vs quieter neighbours
--   center        — uniformly busy
--   Solntsevo     — modest count vs a quiet ring
--   Losiny Ostrov — hole
INSERT INTO spark.base_stations
SELECT
    toInt32(number + 1) AS id,
    55.560 + (cityHash64(number, 11) % 360000) / 1000000. AS lat,
    37.350 + (cityHash64(number, 12) % 520000) / 1000000. AS lon,
    arrayElement([25001, 25002, 25020, 25099], (cityHash64(number, 13) % 4) + 1) AS code,
    toInt32(2000 + intDiv(number, 1000)) AS lac,
    toInt32(number % 1000) AS cellid
FROM numbers(100000);

INSERT INTO spark.gprs_data
SELECT
    toDateTime64('2026-09-15 00:00:00', 3, 'UTC')
        + toIntervalMillisecond(cityHash64(id, k) % 604800000) AS timestamp,
    concat(
        'ID',
        leftPad(toString(id), 6, '0'),
        leftPad(toString(intDiv(k, 4)), 4, '0')
    ) AS identificator,
    code,
    lac,
    cellid
FROM
(
    SELECT
        id,
        code,
        lac,
        cellid,
        toUInt32(
            multiIf(
                d_luzhniki < 0.35, 280 + cityHash64(id) % 80,
                d_luzhniki < 1.10, 30 + cityHash64(id) % 25,
                d_center < 2.20, 100 + cityHash64(id) % 50,
                d_solntsevo < 0.45, 140 + cityHash64(id) % 50,
                d_park < 1.30, 2 + cityHash64(id) % 3,
                25 + cityHash64(id) % 35
            )
        ) AS n_events
    FROM
    (
        SELECT
            id,
            code,
            lac,
            cellid,
            sqrt(pow((lat - 55.7158) * 111.32, 2) + pow((lon - 37.5537) * 62.65, 2)) AS d_luzhniki,
            sqrt(pow((lat - 55.7522) * 111.32, 2) + pow((lon - 37.6173) * 62.65, 2)) AS d_center,
            sqrt(pow((lat - 55.5800) * 111.32, 2) + pow((lon - 37.4700) * 62.65, 2)) AS d_solntsevo,
            sqrt(pow((lat - 55.8700) * 111.32, 2) + pow((lon - 37.7800) * 62.65, 2)) AS d_park
        FROM spark.base_stations
    )
)
ARRAY JOIN range(n_events) AS k;

CREATE VIEW spark.bs_load AS
SELECT
    b.id,
    b.lat,
    b.lon,
    g.n_gprs,
    g.uniq_ident
FROM spark.base_stations AS b
INNER JOIN
(
    SELECT
        code,
        lac,
        cellid,
        count() AS n_gprs,
        uniqExact(identificator) AS uniq_ident
    FROM spark.gprs_data
    WHERE timestamp >= '2026-09-21'
      AND timestamp < '2026-09-22'
    GROUP BY code, lac, cellid
) AS g
    ON b.code = g.code AND b.lac = g.lac AND b.cellid = g.cellid;
