Список узлов — `conf/workers`. Нужен доступ по SSH без пароля: заранее положить ключи в `authorized_keys`.

Для настройки Spark-кластера (и узлов в целом) есть два основных файла в `conf`.

**Файлы с окончанием `.template` лучше не изменять** — это образцы. Рабочие копии создаются без `.template`:

```bash
cp conf/spark-env.sh.template conf/spark-env.sh
cp conf/spark-defaults.conf.template conf/spark-defaults.conf
```

После этого редактировать:

```text
conf/spark-env.sh
conf/spark-defaults.conf
```

Править оба файла необязательно:

- `spark-env.sh` — Master, Workers, ресурсы машин и каталоги.
- `spark-defaults.conf` — чтобы не повторять настройки приложений в каждом `spark-submit`.
- `.template` лучше оставить неизменными как справочные файлы.

---

## `spark-env.sh`

Настройки машин и демонов Master/Worker. Spark читает файл при запуске Master и Workers.

Нужно разместить на всех машинах. Значения могут отличаться, если оборудование или пути разные.

Переменные обычно записываются так:

```bash
export SPARK_MASTER_HOST=10.0.0.10
export SPARK_WORKER_CORES=56
export SPARK_WORKER_MEMORY=440g
export SPARK_LOCAL_DIRS=/nvme/spark-local
```

Ниже — разбор переменных для Standalone-кластера.

## `spark-defaults.conf`

Настройки Spark-приложений по умолчанию. Применяются при запуске через `spark-submit`, если не переопределены явно:

```properties
spark.master spark://10.0.0.10:7077
spark.executor.cores 8
spark.executor.memory 56g
spark.sql.shuffle.partitions 400
spark.eventLog.enabled true
```

Пример переопределения:

```bash
spark-submit --executor-memory 32g application.py
```

Приоритет:

```text
SparkConf в коде
→ параметры spark-submit
→ spark-defaults.conf
```

---

## Настройки Master

### `SPARK_MASTER_HOST`

Адрес, на котором Master принимает подключения:

```bash
export SPARK_MASTER_HOST=10.0.0.10
```

Workers и приложения подключаются к нему:

```text
spark://10.0.0.10:7077
```

Обычно лучше внутренний статический IP или внутреннее DNS-имя. Адрес должен быть доступен со всех Workers.

### `SPARK_MASTER_PORT`

RPC-порт Master:

```bash
export SPARK_MASTER_PORT=7077
```

Используется Workers, драйверами и `spark-submit`:

```bash
spark-submit --master spark://10.0.0.10:7077 ...
```

Это не порт веб-интерфейса.

### `SPARK_MASTER_WEBUI_PORT`

Порт веб-интерфейса Master:

```bash
export SPARK_MASTER_WEBUI_PORT=8080
```

Интерфейс:

```text
http://10.0.0.10:8080
```

Там отображаются Workers, приложения, ресурсы и состояние кластера.

### `SPARK_MASTER_OPTS`

Дополнительные JVM-настройки только для Master. Не передаются драйверам и executors.

```bash
export SPARK_MASTER_OPTS="-Dx=y"
```

Через них настраивают:

- восстановление состояния;
- ZooKeeper;
- параметры журналирования;
- очистку сохранённых данных.

Практические опции — в разделе [SPARK_MASTER_OPTS: тонкая настройка](#spark_master_opts-тонкая-настройка).

---

## Настройки Worker

### `SPARK_WORKER_CORES`

Количество ядер, которое Worker объявляет доступным Spark:

```bash
export SPARK_WORKER_CORES=56
```

Это верхний предел для всех приложений на этой машине, а не количество ядер одного executor.

На машине с 64 ядрами разумно оставить несколько ядер ОС и служебным процессам.

### `SPARK_WORKER_MEMORY`

Количество памяти, которое Worker предоставляет приложениям:

```bash
export SPARK_WORKER_MEMORY=440g
```

Память не выделяется сразу. Worker сообщает Master, сколько памяти разрешено распределить между executors.

Память отдельного executor задаётся отдельно: `spark.executor.memory`.

```text
Worker:   440 ГБ всего
Executor: 56 ГБ
```

На одном Worker может быть несколько executors разных приложений.

Значение по умолчанию — почти вся память машины за вычетом 1 ГиБ. Для сервера с 512 ГБ это слишком агрессивно, поэтому лучше установить ограничение явно.

### `SPARK_WORKER_PORT`

RPC-порт Worker:

```bash
export SPARK_WORKER_PORT=7078
```

По умолчанию выбирается случайный свободный порт. Явное значение удобно при наличии firewall.

На разных физических машинах можно использовать одинаковый порт.

### `SPARK_WORKER_WEBUI_PORT`

Порт веб-интерфейса Worker:

```bash
export SPARK_WORKER_WEBUI_PORT=8081
```

```text
http://worker1:8081
http://worker2:8081
```

Там видны executors, приложения, журналы и использование ресурсов конкретного Worker.

### `SPARK_WORKER_DIR`

Рабочий каталог Worker:

```bash
export SPARK_WORKER_DIR=/data/spark/work
```

В нём находятся:

- каталоги приложений;
- загруженные JAR-файлы;
- файлы executors;
- `stdout` и `stderr`;
- служебные рабочие данные.

Нужно следить за очисткой, иначе старые приложения постепенно займут диск.

### `SPARK_WORKER_OPTS`

Дополнительные JVM-параметры только для Worker. Это не настройки executors: параметры приложений задаются через `spark-submit`, `SparkConf` или `spark-defaults.conf`.

```bash
export SPARK_WORKER_OPTS="-Dx=y"
```

Пример автоматической очистки:

```bash
export SPARK_WORKER_OPTS="\
-Dspark.worker.cleanup.enabled=true \
-Dspark.worker.cleanup.interval=1800 \
-Dspark.worker.cleanup.appDataTtl=604800"
```

---

## Диски и временные данные

### `SPARK_LOCAL_DIRS`

Каталоги для интенсивных временных операций:

```bash
export SPARK_LOCAL_DIRS=/nvme1/spark,/nvme2/spark
```

Там Spark хранит:

- результаты shuffle;
- spill-файлы при нехватке памяти;
- разделы RDD, вытесненные на диск;
- другую временную информацию.

Каталоги должны быть на быстрых локальных SSD/NVMe. Если дисков несколько, их перечисляют через запятую — Spark распределяет данные между ними.

Это не постоянное хранилище. Важные результаты записывают в S3, HDFS, GeoParquet, базу данных или другое внешнее хранилище. Настройка S3: [S3.md](S3.md).

```text
SPARK_LOCAL_DIRS  → тяжёлые временные данные вычислений
SPARK_WORKER_DIR  → рабочие каталоги приложений и executors
```

---

## Журналы и PID

### `SPARK_LOG_DIR`

Каталог журналов Master и Worker:

```bash
export SPARK_LOG_DIR=/var/log/spark
```

По умолчанию: `SPARK_HOME/logs`.

У пользователя Spark должны быть права на запись.

### `SPARK_LOG_MAX_FILES`

Максимальное количество сохраняемых файлов журналов:

```bash
export SPARK_LOG_MAX_FILES=10
```

Старые журналы удаляются или заменяются при ротации. Значение по умолчанию — 5.

### `SPARK_PID_DIR`

Каталог PID-файлов:

```bash
export SPARK_PID_DIR=/var/run/spark
```

PID-файл содержит идентификатор процесса и используется скриптами остановки.

По умолчанию применяется `/tmp`. На production-серверах лучше отдельный постоянный каталог с корректными правами.

---

## Настройки демонов

### `SPARK_DAEMON_MEMORY`

Память JVM для процессов Master и Worker:

```bash
export SPARK_DAEMON_MEMORY=2g
```

Это память только управляющих демонов, не executors и не драйвера. Для кластера из двух Workers обычно достаточно 1–2 ГБ.

### `SPARK_DAEMON_JAVA_OPTS`

Общие JVM-параметры Master и Worker:

```bash
export SPARK_DAEMON_JAVA_OPTS="-Dsome.option=value"
```

Через них задают системные свойства JVM, сетевые параметры или общие настройки отказоустойчивости.

Пример восстановления состояния Master (RocksDB):

```bash
export SPARK_MASTER_HOST=master1
export SPARK_MASTER_PORT=7077
export SPARK_DAEMON_JAVA_OPTS="\
-Dspark.deploy.recoveryMode=ROCKSDB \
-Dspark.deploy.recoveryDirectory=/var/lib/spark/master-recovery \
-Dspark.deploy.recoveryTimeout=60s"
```

### `SPARK_DAEMON_CLASSPATH`

Дополнительный classpath для Master и Worker:

```bash
export SPARK_DAEMON_CLASSPATH=/opt/custom/lib/*
```

Нужен редко — обычно только для специальных плагинов или дополнительных библиотек самих демонов.

Добавление библиотеки сюда не делает её автоматически доступной пользовательским Spark-приложениям.

## `SPARK_PUBLIC_DNS`

Публичное DNS-имя узла:

```bash
export SPARK_PUBLIC_DNS=spark.example.com
```

Используется преимущественно для ссылок и адресов, которые показывает Spark. Не заменяет правильную настройку внутренних адресов, маршрутизации и firewall.

---

## Пример для ваших машин

Для двух одинаковых серверов стартовая конфигурация могла бы выглядеть так:

```bash
export SPARK_MASTER_HOST=10.0.0.10
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

export SPARK_WORKER_CORES=56
export SPARK_WORKER_MEMORY=440g
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081

export SPARK_LOCAL_DIRS=/nvme1/spark-local,/nvme2/spark-local
export SPARK_WORKER_DIR=/nvme1/spark-worker
export SPARK_LOG_DIR=/var/log/spark
export SPARK_PID_DIR=/var/run/spark
export SPARK_DAEMON_MEMORY=2g
```

На первой машине можно запускать Master и Worker, на второй — ещё один Worker. Тогда вычислительные ресурсы обеих машин будут использоваться кластером.

---

## `SPARK_MASTER_OPTS`: тонкая настройка

Документация: [Spark Standalone](https://spark.apache.org/docs/latest/spark-standalone.html).

Для небольшого Standalone-кластера большинство `SPARK_MASTER_OPTS` лучше оставить стандартными. Практически полезны следующие.

Важно объявлять `SPARK_MASTER_OPTS` одной строкой: повторный `export SPARK_MASTER_OPTS=...` перезапишет предыдущее значение. Настройки executors, Workers и Spark SQL сюда помещать не нужно.

### Ограничение приложения по умолчанию

```bash
-Dspark.deploy.defaultCores=96
```

Приложение без `spark.cores.max` получит максимум 96 ядер. Если приложение одно и может использовать все 112 ядер — не задавайте.

### Ограничение количества драйверов

```bash
-Dspark.deploy.maxDrivers=10
```

Защищает Master от одновременного запуска слишком большого количества драйверов. В основном актуально для `cluster` deploy mode.

### Количество записей в Master UI

```bash
-Dspark.deploy.retainedApplications=100
-Dspark.deploy.retainedDrivers=100
```

Ограничивает количество завершённых приложений и драйверов в памяти и интерфейсе Master.

### Защита отключения Workers через UI

```bash
-Dspark.master.ui.decommission.allow.mode=LOCAL
```

Разрешает endpoint `/workers/kill` только с машины Master. Можно полностью запретить:

```bash
-Dspark.master.ui.decommission.allow.mode=DENY
```

### Отключение REST API

Если удалённая отправка через REST не используется:

```bash
-Dspark.master.rest.enabled=false
```

Это закрывает ненужный порт `6066`. Обычный `spark-submit --master spark://...` продолжит работать.

### Ссылка на History Server

Если History Server будет настроен:

```bash
-Dspark.master.ui.historyServerUrl=http://history-server:18080
```

Без History Server параметр не нужен.

### Пример умеренной конфигурации

```bash
export SPARK_MASTER_OPTS="\
-Dspark.deploy.retainedApplications=100 \
-Dspark.deploy.retainedDrivers=100 \
-Dspark.master.ui.decommission.allow.mode=LOCAL \
-Dspark.master.rest.enabled=false"
```

Если хотите ограничить приложения 96 ядрами, добавьте:

```bash
-Dspark.deploy.defaultCores=96
```

---

## Исходные ресурсы

Две машины:

```text
Каждая: 64 CPU, 512 ГБ RAM
Всего:  128 CPU, 1024 ГБ RAM
```

Не следует отдавать Spark абсолютно всё. Нужен запас для ОС, файлового кеша, Python и native-памяти.

## Настройка Workers

На каждой машине в `spark-env.sh`:

```bash
export SPARK_WORKER_CORES=56
export SPARK_WORKER_MEMORY=440g
export SPARK_LOCAL_DIRS=/путь/к/NVMe/spark-local
```

Получается:

```text
Один Worker: 56 CPU, 440 ГБ
Два Workers: 112 CPU, 880 ГБ
```

Это общий пул ресурсов, из которого Master размещает executors.

## Настройка одного executor

В `spark-defaults.conf`:

```properties
spark.executor.cores 8
spark.executor.memory 56g
```

Один executor получает:

```text
8 ядер
56 ГБ JVM heap
```

Фактическое потребление памяти будет выше из-за:

- Python-процессов;
- native-памяти;
- Sedona/JTS;
- сетевых буферов;
- off-heap данных;
- служебных структур JVM.

Поэтому оставленный запас памяти важен.

## Количество executors

Для одного Worker:

```text
По CPU:    floor(56 / 8) = 7
По памяти: floor(440 / 56) = 7
Итого:     min(7, 7) = 7 executors
```

Для двух Workers:

```text
7 × 2 = 14 executors
14 × 8 = 112 используемых ядер
14 × 56 = 784 ГБ executor heap
```

На каждой физической машине останется:

```text
512 − (7 × 56) = 120 ГБ
```

Этот запас покрывает ОС и память вне executor heap.

## Ограничение приложения

Если приложение может использовать весь кластер, `spark.cores.max` можно не указывать. Тогда оно сможет получить:

```text
14 executors × 8 CPU = 112 CPU
```

Если нужно оставить ресурсы другим приложениям:

```properties
spark.cores.max 96
```

Тогда:

```text
floor(96 / 8) = 12 executors
```

Что за что отвечает:

```text
spark.executor.cores   → ядра одного executor
spark.executor.memory  → heap одного executor
spark.cores.max        → максимум ядер всего приложения
SPARK_WORKER_CORES     → общий CPU-пул одной машины
SPARK_WORKER_MEMORY    → общий пул памяти одной машины
```

## Значение на Master

Опциональный лимит для приложений, которые не указали `spark.cores.max`:

```bash
export SPARK_MASTER_OPTS="\
-Dspark.deploy.defaultCores=96"
```

Нужен преимущественно на общем кластере. Для одного основного приложения можно не задавать. Подробности и как совместить с другими опциями — в разделе [SPARK_MASTER_OPTS: тонкая настройка](#spark_master_opts-тонкая-настройка).

## Переопределение при запуске

Значения из `spark-defaults.conf` можно изменить для конкретного приложения:

```bash
spark-submit \
  --executor-cores 4 \
  --executor-memory 32g \
  --conf spark.cores.max=80 \
  application.py
```

## Итоговая стартовая конфигурация

`spark-env.sh` на Workers:

```bash
export SPARK_WORKER_CORES=56
export SPARK_WORKER_MEMORY=440g
export SPARK_LOCAL_DIRS=/путь/к/NVMe/spark-local
```

`spark-defaults.conf`:

```properties
spark.executor.cores 8
spark.executor.memory 56g
```

На старте можно не задавать:

```text
spark.cores.max
spark.deploy.defaultCores
Dynamic Allocation
Stage-level scheduling
```

В таком варианте одно приложение сможет использовать до 14 executors — по 7 на каждой машине.


Example for defaults template:
```
spark.master                     spark://localhost:7077

spark.driver.cores               1
spark.driver.memory              2g
spark.driver.maxResultSize       1g

spark.executor.cores             2
spark.executor.memory            2g
spark.cores.max                  8

spark.sql.adaptive.enabled       true

spark.eventLog.enabled           true
spark.eventLog.dir              file:///Users/vladislavkotletkin/development/spark-eventlog
spark.history.fs.logDirectory   file:///Users/vladislavkotletkin/development/spark-eventlog

# Kryo + Java 17/21 (cluster deploy-mode). Required for Sedona.
spark.driver.extraJavaOptions    --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
spark.executor.extraJavaOptions  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```