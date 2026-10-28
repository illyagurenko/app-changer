# App Changer Service

Микросервис асинхронного процессинга и расчета банковских балансов по счетам с интеграцией через Apache Kafka и хранимые процедуры PostgreSQL.

Реализован на **Java 25** и **Quarkus 3.26** с использованием **Hibernate ORM with Panache** и **Liquibase**.

---

## Архитектура и схема работы

Сервис функционирует по принципу конвейера (Pipeline) в трех независимых изолированных потоках:

```text
  [ Kafka Topic IN ]
          │
          ▼  (ConsumeWorker)
  ┌──────────────────────────────────────────────────────────────────┐
  │  APP_CHANGER_TRANS (STATUS: 'RECEIVED')                          │
  │  └──► Аудит: APP_CHANGER_IO_MSGS (DIR: 'IN')                     │
  └──────────────────────────────────────────────────────────────────┘
          │
          ▼  (ProcessingWorker: FOR UPDATE SKIP LOCKED)
  ┌──────────────────────────────────────────────────────────────────┐
  │  [ Валидация APP_CONTROLLER ] ──► [ Процедура changer_data_account ]
  └──────────────────────────────────────────────────────────────────┘
          │
          ▼  (Формирование ответа resp_data)
  ┌──────────────────────────────────────────────────────────────────┐
  │  APP_CHANGER_TRANS (STATUS: 'READY_TO_SEND')                     │
  └──────────────────────────────────────────────────────────────────┘
          │
          ▼  (ProduceWorker: FOR UPDATE SKIP LOCKED)
  [ Kafka Topic OUT ]
          │
          ▼  (Финализация)
  ┌──────────────────────────────────────────────────────────────────┐
  │  APP_CHANGER_TRANS (STATUS: 'COMPLETED')                         │
  │  └──► Аудит: APP_CHANGER_IO_MSGS (DIR: 'OUT')                    │
  └──────────────────────────────────────────────────────────────────┘
```

---

## Стек технологий

* **Java**: 25 
* **Фреймворк**: Quarkus 3.26.4
* **ORM и БД**: Hibernate ORM with Panache, PostgreSQL 16 
* **Миграции БД**: Liquibase
* **Очереди**: Apache Kafka
* **Сериализация**: Jackson (Java Records DTO)

---

## Требования для локального запуска

1. **JDK 25** (установленная переменная окружения `JAVA_HOME`).
2. **Docker** и **Docker Compose**.

---

## Быстрый старт

### 1. Запуск инфраструктуры (БД и Kafka)
В корневой папке проекта выполните:
```bash
docker compose up -d
```
Будут запущены:
* **PostgreSQL** на порту `5432` (База: `changer_db`, пользователь: `changer_user`, пароль: `root`).
* **Kafka** на порту `9092` (режим KRaft без Zookeeper).

### 2. Запуск приложения в Dev-режиме
При старте автоматически накатятся миграции Liquibase:
```bash
./gradlew quarkusDev
```
*Флаг `--add-opens=java.base/java.lang=ALL-UNNAMED` уже прописан в `build.gradle` и передается автоматически.*

### 3. Запуск приложения напрямую из IntelliJ IDEA
Если вы запускаете класс `ru.itone.illya4gurenko.AppChangerMain` зеленой кнопкой Run:
1. Перейдите в меню `Run` -> `Edit Configurations...` -> выберите `AppChangerMain`.
2. Нажмите `Modify options` -> `Add VM options`.
3. Вставьте в поле VM options:
   ```text
   --add-opens=java.base/java.lang=ALL-UNNAMED
   ```

---

## Сборка и запуск в Docker

### Сборка Fast-JAR
```bash
./gradlew build
```

### Сборка Docker-образа
```bash
docker build -f src/main/docker/Dockerfile.jvm -t app-changer:1.0.0 .
```

### Запуск контейнера
```bash
docker run -i --rm -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS="host.docker.internal:9092" \
  -e QUARKUS_DATASOURCE_JDBC_URL="jdbc:postgresql://host.docker.internal:5432/changer_db" \
  app-changer:1.0.0
```

---

## Форматы сообщений

### 1. Входящий запрос (Kafka IN: `gru.balance.request`)
```json
{
  "actualTimestamp": 1729000000000,
  "systemId": "GRU",
  "requestId": "req-9f8e7d6c-5b4a",
  "eventType": "BALANCE",
  "entityType": "ACCOUNT",
  "events": [
    {
      "id": "1",
      "systemAccount": "40817810099910004321",
      "currency": "RUB",
      "XALFA": 2500.00,
      "operation": "CR"
    }
  ]
}
```

### 2. Исходящий ответ (Kafka OUT: `gru.balance.response`)
```json
{
  "actualTimestamp": 1729000001500,
  "systemId": "SMART",
  "requestId": "req-9f8e7d6c-5b4a",
  "eventType": "BALANCE",
  "entityType": "ACCOUNT",
  "status": "SUCCESS",
  "events": [
    {
      "entityValue": "40817810099910004321",
      "entityId": 1,
      "status": "SUCCESS",
      "data": {
        "oldTbal": 15000.00,
        "newTbal": 17500.00,
        "operation": "CR"
      }
    }
  ]
}
```

---

## Тестирование

### Запуск интеграционных тестов
```bash
./gradlew test