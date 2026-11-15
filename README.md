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

Сервис поддерживает работу как в JVM-режиме, так и в **GraalVM Native Image** (скомпилированный машинный бинарник Linux ELF).

### 1. Сборка Native Image (GraalVM / Mandrel 25)

Для сборки нативного бинарника под Linux используется официальный Mandrel-билдер под Java 25:
```bash
./gradlew build \
  "-Dquarkus.native.enabled=true" \
  "-Dquarkus.package.jar.enabled=false" \
  "-Dquarkus.native.container-build=true" \
  "-Dquarkus.native.builder-image=quay.io/quarkus/ubi10-quarkus-mandrel-builder-image:jdk-25" \
  -x test
```
После компиляции в папке `build/` создается исполняемый файл `app-changer-1.0.0-runner`.

### 2. Сборка Docker-образа
```bash
# Сборка Native-образа на базе ubi9-minimal (без JVM, ~150 МБ)
docker build -t app-changer:latest .
```

### 3. Запуск в Docker Compose (2 экземпляра)

Сервис разворачивается вместе с PostgreSQL и Apache Kafka в сети `food_bridge` в двух параллельных экземплярах:

```bash
docker compose up -d
```

Инстансы и порты:
* **`app_changer_1` (Native):** порт `8080`, `APP_NODE_ID: 161`
* **`app_changer_2` (Native):** порт `8081`, `APP_NODE_ID: 162`
* **Потребление ресурсов:** ~55–60 MiB RAM на инстанс, холодный старт за 1.2 секунды.
* **Балансировка:** 4 партиции топиков Kafka (`gru.balance.request` и `gru.balance.response`) равномерно распределяются между обеими нодами.

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

Проект содержит модульные и интеграционные тесты с краткими названиями `@DisplayName` в нижнем регистре и покрытием кода (Jacoco >= 80%).

* **Модульные тесты (`src/test/java`):** изолированное тестирование сервисов (`ProcessingService`, `ProduceService`, `ConsumeService`), воркеров, утилит сериализации с Mockito.
* **Интеграционные тесты (`src/IntegrationTest/java`):**
  * Проверка успешного поднятия контекста Quarkus (`@QuarkusTest`).
  * Сквозное тестирование консьюминга, процессинга и продюсинга ответов с проверкой позитивного и негативного исходов (проверка отклонения и маршрутизации).

```bash
# Запуск модульных тестов с отчетом покрытия Jacoco
./gradlew test

# Запуск интеграционных тестов
./gradlew integrationTest
```