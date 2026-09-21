Какие ресурсы отдавать Spark относительно системы:
[SYSTEM_SETTINGS.md](SYSTEM_SETTINGS.md)

Все настройки, кто и где будут воркеры - conf/workers
Надо - чтобы был доступ по SSH без пароля. Тоесть заранее ключи в authorized keys стоит подкинуть
Так же нужно настроить конфигурацию тачек.
Подробнее: в [CONF.md](CONF.md)

S3 / object storage: [S3.md](S3.md). Локально — SeaweedFS + ClickHouse: [docker-compose.yml](../docker-compose.yml)
(`docker compose up -d`). ClickHouse: `http://localhost:8123`, user/db `spark`, password `sparksecret`.
JAR-ы в `$SPARK_HOME/jars` на всех узлах:
- `installation/spark-cloud-jars/` (S3)
- `installation/spark-sedona-jars/`
- `installation/spark-clickhouse-jars/`
- `installation/spark-graphframes-jars/`

Полный `--packages` (если JAR-ы не класть в `$SPARK_HOME/jars`): [START.md](START.md).

Кейсы GraphFrames / SQL по биллингу и БС: [ALGORITHMS.md](ALGORITHMS.md).

1) ./sbin/start-master.sh
2) ./sbin/start-worker.sh localhost:7077

Create script

3)[START.md](START.md) For start job
