# VK group post parser

Проект по инфраструктуре больших данных. Подготовка локальной среды сбора данных для последующей обработки ML.

Регистрация debezium connector

```bash
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres.json
```

Сборка Docker контейнера для парсинга постов VK

```bash
docker build --tag vk-post-parsing:latest -f Dockerfile-post-parser ./
docker build --tag vk-post-processing:latest -f Dockerfile-post-processing ./
docker build --tag vk-url-image-parsing:latest -f Dockerfile-image-parser ./
```