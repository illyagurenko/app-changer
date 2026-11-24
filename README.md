# App Changer Service

Высокопроизводительный микросервис асинхронного процессинга и расчета банковских балансов по счетам с динамической маршрутизацией через **app-dispatcher**, очередями **Apache Kafka** и выполнением хранимых процедур в целевых базах данных **PostgreSQL**.

Реализован на **Java 25** и **Quarkus 3.26.4** с полной поддержкой компиляции в **GraalVM Native Image** (Mandrel 25, UBI 10), **Hibernate ORM with Panache** и **Liquibase**.

---

## Архитектура и схема работы

Сервис функционирует по принципу конвейера (Pipeline) в трех независимых изолированных фоновых воркерах, поддерживающих распределенную обработку через `FOR UPDATE SKIP LOCKED`:

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
        1. Запрос роутинга счетов (HTTP POST + AES Basic Auth)
                                 ▼
                     ┌───────────────────────┐
                     │    app-dispatcher     │ (порт 8085)
                     └───────────────────────┘
                                 │
            ┌────────────────────┴────────────────────┐
            ▼                                         ▼
   [ jdbc_url пустой/null ]                 [ jdbc_url найден ]
            │                                         │
            ▼                                         ▼
   Ошибка: EMPTY_JDBC_URL                    Подключение к целевой БД
   ("Не найден JDBC URL для счета")        (DynamicDataSourceService, timeout 3s)
                                                      │
                                           ┌──────────┴──────────┐
                                           ▼                     ▼
                                  [ База недоступна ]   [ База доступна ]
                                           │                     │
                                           ▼                     ▼
                                 Ошибка: DB_UNREACHABLE   Вызов процедуры
                                 ("Целевая БД...")        changer_data_account
                                           │                     │
            ┌──────────────────────────────┴─────────────────────┘
            ▼
   ┌───────────────────────────────────────────────────────────┐
   │  Таблица APP_CHANGER_TRANS (STATUS: 'READY_TO_SEND')      │
   │  (Сформирован JSON-ответ: SUCCESS или статус с ошибками)  │
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

* **Язык:** Java 25 (Records, Pattern Matching, Foreign Function & Memory API, Virtual Threads ready)
* **Фреймворк:** Quarkus 3.26.4 (Quarkus REST, Jackson, Panache, Agroal)
* **Нативная компиляция:** Mandrel / GraalVM (базовый сборочный образ `ubi10-quarkus-mandrel-builder-image:jdk-25`, рантайм `ubi9-minimal:9.5`)
* **Базы данных:** PostgreSQL 16 (с поддержкой георезервирования и динамических источников данных)
* **Криптография:** AES/ECB/PKCS5Padding (128/256 бит Base64) для дешифрования секретов и Basic Auth
* **ORM и миграции:** Hibernate ORM with Panache, Liquibase
* **Очереди сообщений:** Apache Kafka (KRaft режим)
* **Сборщик:** Gradle 9.1

---

## Порты и сетевая архитектура

Все сервисы объединены через общую внешнюю Docker-сеть `food_bridge`:

| Контейнер / Сервис | Назначение | Порт хоста | Внутренний порт |
| :--- | :--- | :--- | :--- |
| **`app_changer_1`** | Экземпляр 1 (Нода 161, Native / JVM) | `8080` | `8080` |
| **`app_changer_2`** | Экземпляр 2 (Нода 162, Native / JVM) | `8081` | `8080` |
| **`app-dispatcher`** | Сервис диспетчеризации и роутинга счетов | `8085` | `8085` |
| **`postgres_changer`**| База данных PostgreSQL для app-changer | `5432` | `5432` |
| **`test_kafka`** | Брокер Kafka (KRaft режим) | `9092` | `9092`, `29092` |

---

## Интеграция с сервисом app-dispatcher

Сервис `app-changer` динамически запрашивает маршрутизацию для всех уникальных счетов из входящего пакета (`events`) у сервиса `app-dispatcher`:

### 1. Безопасность и авторизация (AES + HTTP Basic)
* Запросы отправляются по протоколу HTTP с заголовками `Authorization: Basic <base64>` и дублирующим `Basic: <base64>`.
* Пароль учетной записи хранится в зашифрованном виде (Base64) без префикса `ENC(...)`.
* Сервис `AESCryptoService` расшифровывает пароль на лету с использованием симметричного ключа (`crypto.key` или файла `crypto.key.path`).
* Дефолтный мастер-ключ AES: `vtkFKhCzuberXpROnkV/2Q==`.

### 2. Логика маршрутизации и обработка ошибок
1. **`EMPTY_JDBC_URL`:**
   Если `app-dispatcher` вернул пустой или `null` адрес JDBC URL для счета, обработка счета прерывается со статусом `EMPTY_JDBC_URL` и сообщением *"Не найден JDBC URL для счета"*.
2. **`DB_UNREACHABLE`:**
   При попытке установить соединение с целевой БД используется быстрый таймаут подключения (3 секунды). Если база данных недоступна, событие помечается кодом `DB_UNREACHABLE` и сообщением *"Целевая БД недоступна"*.
3. **`SUCCESS`:**
   При успешном подключении вызывается хранимая процедура `changer_data_account(cur_in, cur_out)`. Рассчитанные остатки (`CR`, `DR`, `ZR`) и реквизиты записываются в результат со статусом `SUCCESS`. Если `jdbcUrl` совпадает с локальной БД сервиса, соединение повторно берется из дефолтного пула `AgroalDataSource`.

---

## Георезервирование (High Availability)

Сервис поддерживает автоматический failover для БД и брокеров Kafka без перезапуска приложения:

1. **PostgreSQL Multi-Host Failover:**
   Поддерживает указание Primary и Standby узлов PostgreSQL в строке подключения:
   `jdbc:postgresql://${PG_HOST_PRIMARY}:${PG_PORT_PRIMARY},${PG_HOST_STANDBY}:${PG_PORT_STANDBY}/changer_db?targetServerType=primary`
2. **Kafka Dual-Broker Failover:**
   Клиент подключается к списку брокеров: `${KAFKA_BROKER_PRIMARY},${KAFKA_BROKER_STANDBY}`.

---

## Переменные окружения и конфигурация

Все ключевые параметры настраиваются через переменные окружения (в файле `.env` или через Docker Compose):

| Переменная | Значение по умолчанию | Описание |
| :--- | :--- | :--- |
| `PG_HOST_PRIMARY` | `localhost` / `postgres_changer` | Основной хост БД PostgreSQL |
| `PG_PORT_PRIMARY` | `5432` | Порт основного хоста PostgreSQL |
| `PG_HOST_STANDBY` | `localhost` / `postgres_changer` | Резервный хост БД PostgreSQL |
| `PG_PORT_STANDBY` | `5432` | Порт резервного хоста PostgreSQL |
| `KAFKA_BROKER_PRIMARY` | `localhost:9092` / `test_kafka:29092` | Основной брокер Kafka |
| `KAFKA_BROKER_STANDBY` | `localhost:9092` / `test_kafka:29092` | Резервный брокер Kafka |
| `DISPATCHER_URL` | `http://localhost:8085/get-datasoure-by-account` | URL эндпоинта роутинга счетов |
| `DISPATCHER_USERNAME`| `APP_CHANGER` | Имя пользователя для Basic Auth |
| `DISPATCHER_PASSWORD`| `xP7Llfm/e2eItNeGlxNYnQ==` | Зашифрованный AES пароль для Basic Auth |
| `CRYPTO_KEY` | `vtkFKhCzuberXpROnkV/2Q==` | Base64 AES-ключ шифрования |
| `APP_NODE_ID` | `161` / `162` | Идентификатор узла кластера для логов и аудита |

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

## Пошаговая инструкция по сборке и запуску (Clone & Run Guide)

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

DISPATCHER_URL=http://app-dispatcher:8085/get-datasoure-by-account
DISPATCHER_USERNAME=APP_CHANGER
DISPATCHER_PASSWORD=xP7Llfm/e2eItNeGlxNYnQ==
CRYPTO_KEY=vtkFKhCzuberXpROnkV/2Q==
```

### Шаг 3. Создание общей Docker-сети
Все микросервисы (`publisher`, `adapter`, `dispatcher`, `changer`) взаимодействуют через общую Docker-сеть `food_bridge`. Создайте её:
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

### Шаг 5. Сборка GraalVM Native Image (Mandrel 25)

Сборка компилирует приложение в нативный машинный код Linux ELF внутри официального контейнера `ubi10-quarkus-mandrel-builder-image:jdk-25`. Локально установленный GraalVM **не требуется**.

> **Требования к памяти:** Docker Desktop должен иметь выделено не менее **6–8 ГБ оперативной памяти**.

Выполните команду сборки:

* **Для Windows (PowerShell / CMD):**
  ```powershell
  .\gradlew.bat build "-Dquarkus.native.enabled=true" "-Dquarkus.package.jar.enabled=false" "-Dquarkus.native.container-build=true" "-Dquarkus.native.builder-image=quay.io/quarkus/ubi10-quarkus-mandrel-builder-image:jdk-25" -x test
  ```

* **Для Linux / macOS:**
  ```bash
  ./gradlew build "-Dquarkus.native.enabled=true" "-Dquarkus.package.jar.enabled=false" "-Dquarkus.native.container-build=true" "-Dquarkus.native.builder-image=quay.io/quarkus/ubi10-quarkus-mandrel-builder-image:jdk-25" -x test
  ```

По завершении сборки в каталоге `build/` появится бинарный исполняемый файл:
`build/app-changer-1.0.0-runner` (~120–135 МБ).

---

### Шаг 6. Сборка Docker-образа и запуск инстансов

1. **Сборка нативного Docker-образа на базе UBI Micro/Minimal:**
   ```bash
   docker build -f src/main/docker/Dockerfile.native -t app-changer:latest .
   ```
   *(Или `docker compose build app-changer-1 app-changer-2` при использовании соответствующего Dockerfile в compose).*

2. **Запуск сервисов:**
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
   INFO [io.quarkus] (main) app-changer 1.0.0 native (powered by Quarkus 3.26.4) started in 0.045s. Listening on: http://0.0.0.0:8080
   ```

3. **Проверка REST API:**
   ```bash
   curl -i http://localhost:8080/api/io-msgs/latest
   ```

---

### Альтернатива: Локальный запуск в режиме разработки (Dev Mode)

Для быстрой отладки в JVM с горячей перезагрузкой (Hot-Reload):
```bash
./gradlew quarkusDev
```
Миграции Liquibase накатятся автоматически при старте.
