# App Changer Service

Высокопроизводительный микросервис асинхронного процессинга и расчета банковских балансов по счетам с интеграцией через Apache Kafka и хранимые процедуры PostgreSQL.

Реализован на **Java 25** и **Quarkus 3.26.4** с поддержкой компиляции в **GraalVM Native Image** (Mandrel 25), **Hibernate ORM with Panache** и **Liquibase**.

---

## Архитектура и схема работы

Сервис функционирует по принципу конвейера (Pipeline) в трех независимых изолированных потоках:

```text
              [ Kafka Topic IN: gru.balance.request ]
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │     ConsumeWorker     │
                     └───────────────────────┘
                                 │
                                 ▼
   ┌───────────────────────────────────────────────────────────┐
   │  Таблица APP_CHANGER_TRANS (STATUS: 'RECEIVED')           │
   │  └──► Аудит: APP_CHANGER_IO_MSGS (DIR: 'IN')              │
   └───────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │   ProcessingWorker    │ (FOR UPDATE SKIP LOCKED)
                     └───────────────────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
       [ AppControllerClient ]         [ Процедура changer_data_account ]
       Проверка счетов и роутинга      Расчет остатков (CR, DR, ZR)
                 └───────────────┬───────────────┘
                                 │
                                 ▼
   ┌───────────────────────────────────────────────────────────┐
   │  Таблица APP_CHANGER_TRANS (STATUS: 'READY_TO_SEND')      │
   │  (Сформирован JSON-ответ: SUCCESS или ERROR)              │
   └───────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │     ProduceWorker     │ (FOR UPDATE SKIP LOCKED)
                     └───────────────────────┘
                                 │
                                 ▼
              [ Kafka Topic OUT: gru.balance.response ]
                                 │
                                 ▼
   ┌───────────────────────────────────────────────────────────┐
   │  Таблица APP_CHANGER_TRANS (STATUS: 'COMPLETED')          │
   │  └──► Аудит: APP_CHANGER_IO_MSGS (DIR: 'OUT')             │
   └───────────────────────────────────────────────────────────┘
```

---

## Стек технологий

* **Язык:** Java 25 (Records, Pattern Matching, Foreign Function & Memory API)
* **Фреймворк:** Quarkus 3.26.4 (Quarkus REST, Jackson, Panache)
* **Нативная компиляция:** Mandrel / GraalVM (образ `ubi10-quarkus-mandrel-builder-image:jdk-25`)
* **База данных:** PostgreSQL 16 (с мульти-хост георезервированием)
* **ORM и миграции:** Hibernate ORM with Panache, Liquibase
* **Очереди сообщений:** Apache Kafka (KRaft режим)
* **Сборщик:** Gradle 9.1

---

## Порты и сетевая архитектура

Все сервисы объединяются через общую Docker-сеть `food_bridge`:

| Контейнер | Назначение | Порт хоста | Внутренний порт |
| :--- | :--- | :--- | :--- |
| **`app_changer_1`** | Экземпляр 1 (Нода 161, Native) | `8080` | `8080` |
| **`app_changer_2`** | Экземпляр 2 (Нода 162, Native) | `8081` | `8080` |
| **`postgres_changer`** | База данных PostgreSQL | `5432` | `5432` |
| **`test_kafka`** | Брокер Kafka (KRaft) | `9092` | `9092`, `29092` |

---

## Георезервирование (High Availability)

Сервис поддерживает автоматический failover для БД и брокеров очередей без перезапуска приложения:

1. **PostgreSQL Multi-Host Failover:**
   Поддерживает указание Primary и Standby узлов PostgreSQL в строке подключения:
   `jdbc:postgresql://${PG_HOST_PRIMARY}:${PG_PORT_PRIMARY},${PG_HOST_STANDBY}:${PG_PORT_STANDBY}/changer_db?targetServerType=primary`
2. **Kafka Dual-Broker Failover:**
   Клиент подключается к списку брокеров: `${KAFKA_BROKER_PRIMARY},${KAFKA_BROKER_STANDBY}`.

Переменные настраиваются в файле `.env` в корне проекта. Пример `.env`:
```properties
PG_HOST_PRIMARY=postgres_changer
PG_PORT_PRIMARY=5432
PG_HOST_STANDBY=postgres_changer
PG_PORT_STANDBY=5432

KAFKA_BROKER_PRIMARY=test_kafka:29092
KAFKA_BROKER_STANDBY=test_kafka:29092
```

---

## REST API сервиса

Базовый адрес: `http://localhost:8080` (экземпляр 1) или `http://localhost:8081` (экземпляр 2).

| Метод | Путь | Параметры | Описание |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/io-msgs/latest` | `dir=IN` или `dir=OUT` (по умолч. `OUT`) | Получить DTO последней зафиксированной в аудите записи из `APP_CHANGER_IO_MSGS` |
| `GET` | `/api/io-msgs/by-request` | `requestId` (обязательный), `dir` | Найти запись аудита по конкретному идентификатору пакета `requestId` |

Пример ответа:
```json
{
  "id": 282,
  "transId": 141,
  "requestId": "7916d03a-82aa-45bc-9975-5930ab533111",
  "protocolType": "KAFKA",
  "msgType": "GRU",
  "dir": "OUT",
  "nodeId": "162",
  "insTs": "2026-11-19T08:13:01.155098Z"
}
```

---

## Пошаговая инструкция по запуску с нуля (Clone & Run Guide)

### Шаг 1. Клонирование репозитория
```bash
git clone https://github.com/illyagurenko/app-changer
cd app-changer
```

### Шаг 2. Настройка переменных окружения (`.env`)
Создайте файл `.env` в корне проекта (или скопируйте из `.env.example`):
```bash
cp .env.example .env
```
Пример содержимого `.env`:
```properties
PG_HOST_PRIMARY=postgres_changer
PG_PORT_PRIMARY=5432
PG_HOST_STANDBY=postgres_changer
PG_PORT_STANDBY=5432

KAFKA_BROKER_PRIMARY=test_kafka:29092
KAFKA_BROKER_STANDBY=test_kafka:29092
```

### Шаг 3. Создание общей Docker-сети
Все микросервисы (`publisher`, `adapter`, `changer`) взаимодействуют через внешнюю Docker-сеть `food_bridge`. Создайте её, если она еще не создана:
```bash
docker network create food_bridge
```

### Шаг 4. Запуск инфраструктуры (PostgreSQL и Kafka)
Перед сборкой нативного приложения или запуском тестов поднимите БД и Kafka:
```bash
docker compose up -d postgres-changer kafka
```
*Убедитесь, что контейнеры `postgres_changer` и `test_kafka` имеют статус Up (`docker compose ps`).*

---

### Шаг 5. Сборка Native Image (GraalVM / Mandrel)

Сборка компилирует приложение в нативный машинный код Linux ELF. Для этого **не требуется** локально установленный GraalVM — компиляция происходит внутри официального Docker-контейнера Mandrel.

> **Требования к памяти:** Docker Desktop должен иметь выделено не менее **6–8 ГБ оперативной памяти**, так как нативный компилятор интенсивно анализирует дерево вызовов.

Выполните команду сборки:

* **Для Windows (PowerShell / CMD):**
  ```powershell
  .\gradlew.bat build "-Dquarkus.native.enabled=true" "-Dquarkus.package.jar.enabled=false" "-Dquarkus.native.container-build=true" "-Dquarkus.native.builder-image=quay.io/quarkus/ubi10-quarkus-mandrel-builder-image:jdk-25" -x test
  ```

* **Для Linux / macOS:**
  ```bash
  ./gradlew build "-Dquarkus.native.enabled=true" "-Dquarkus.package.jar.enabled=false" "-Dquarkus.native.container-build=true" "-Dquarkus.native.builder-image=quay.io/quarkus/ubi10-quarkus-mandrel-builder-image:jdk-25" -x test
  ```

Сборка занимает 4–7 минут. По завершении в директории `build/` появится исполняемый файл:
`build/app-changer-1.0.0-runner` (~120–130 МБ).

---

### Шаг 6. Сборка Docker-образа и запуск двух инстансов

1. Соберите легковесный Docker-образ на базе Red Hat UBI Micro/Minimal (~150 МБ):
   ```bash
   docker compose build app-changer-1 app-changer-2
   ```

2. Запустите оба экземпляра сервиса:
   ```bash
   docker compose up -d
   ```

---

### Шаг 7. Проверка работоспособности

1. **Проверка состояния контейнеров:**
   ```bash
   docker compose ps
   ```
   Должны быть запущены: `app_changer_1`, `app_changer_2`, `postgres_changer`, `test_kafka`.

2. **Проверка логов:**
   ```bash
   docker logs -f app_changer_1
   ```
   Ожидаемый вывод:
   ```text
   INFO [io.quarkus] (main) app-changer 1.0.0 native (powered by Quarkus 3.26.4) started in 1.250s. Listening on: http://0.0.0.0:8080
   ```

3. **Проверка REST API:**
   ```bash
   curl.exe -i http://localhost:8080/api/io-msgs/latest
   ```

---

### Альтернатива: Локальный запуск в режиме разработки (Dev Mode)

Если вы хотите запустить сервис без сборки нативного бинарника (для отладки в JVM):
```bash
./gradlew quarkusDev
```
Миграции Liquibase накатятся автоматически при старте.
