Какие ресурсы отдавать Spark относительно системы:
[SYSTEM_SETTINGS.md](SYSTEM_SETTINGS.md)

Все настройки, кто и где будут воркеры - conf/workers
Надо - чтобы был доступ по SSH без пароля. Тоесть заранее ключи в authorized keys стоит подкинуть
Так же нужно настроить конфигурацию тачек.
Подробнее: в [CONF.md](CONF.md)

S3 / MinIO / object storage: [S3.md](S3.md)

1) ./sbin/start-master.sh
2) ./sbin/start-worker.sh localhost:7077

Create script

3)[START.md](START.md) For start job
