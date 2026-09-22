# ClickHouse → каталог Spark

Каталог читает таблицу. `JOIN` из `spark.sql` выполняется в Spark и тянет обе стороны целиком.
Тяжёлый джойн и `GROUP BY` лежат во представлении. Spark читает только его.

`gprs_data` должна быть `PARTITION BY toYYYYMMDD(timestamp)` и `ORDER BY (code, lac, cellid, timestamp, identificator)`.
Иначе фильтр по дню читает всю историю. `base_stations` в запросе нужна только колонками `id, lat, lon, code, lac, cellid`.

```sql
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
    ON b.code = g.code AND b.lac = g.lac AND b.cellid = g.cellid
SETTINGS optimize_aggregation_in_order = 1;
```

В Spark:

```sql
SELECT id, lat, lon, uniq_ident, ST_Point(lon, lat) AS geometry
FROM clickhouse.spark.bs_load
```

Представление считается заново на каждое чтение. Если один и тот же день читается много раз, результат лучше сложить в `MergeTree` через `INSERT SELECT`.
