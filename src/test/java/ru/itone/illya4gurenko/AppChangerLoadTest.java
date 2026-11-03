package ru.itone.illya4gurenko;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.config.AppConfig;
import ru.itone.illya4gurenko.dao.ConfigDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * нагрузочный интеграционный тест внутри контекста quarkus
 */
@QuarkusTest
public class AppChangerLoadTest {
    private static final Logger log = LoggerFactory.getLogger(AppChangerLoadTest.class);

    @Inject
    AppConfig appConfig;

    @Inject
    ConfigDao configDao;

    @Inject
    TransDao transDao;

    private static final int TOTAL_MESSAGES = Integer.getInteger("load.count", 100_000);

    @Test
        //@Disabled("нагрузочное тестирование при поднятой кафке и бд")
    void runHeavyLoadTest() throws InterruptedException {
        AppChangerConfig config = configDao.findBySystemId("GRU");
        Assertions.assertNotNull(config, "config for GRU must exist in database");

        String topicIn = config.protocolIn;
        String topicOut = config.protocolOut;

        log.info("starting load test inside quarkus context: target={} messages, in={}, out={}",
                TOTAL_MESSAGES, topicIn, topicOut);

        CountDownLatch receiveLatch = new CountDownLatch(TOTAL_MESSAGES);
        AtomicLong consumedCount = new AtomicLong(0);
        AtomicBoolean isRunning = new AtomicBoolean(true);

        ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();

        // запуск тестового слушателя на выходном топике
        consumerExecutor.submit(() -> {
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getKafkaBootstrapServers());
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID());
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(topicOut));

                while (isRunning.get() && receiveLatch.getCount() > 0) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (var record : records) {
                        if (record.value() != null && record.value().contains("\"status\":\"SUCCESS\"")) {
                            consumedCount.incrementAndGet();
                        }
                        receiveLatch.countDown();
                    }
                }
            } catch (Exception e) {
                log.error("error in test consumer", e);
            }
        });

        // пауза на подписку консьюмера
        Thread.sleep(1500);

        // отправка сообщений в Kafka
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getKafkaBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        Instant start = Instant.now();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                String reqId = "load-req-" + i;
                String payload = """
                    {"actualTimestamp":%d,"systemId":"GRU","requestId":"%s","eventType":"BALANCE","entityType":"ACCOUNT","events":[{"id":"%d","systemAccount":"408178100999%08d","currency":"RUB","XALFA":100.00,"operation":"CR"}]}
                    """.formatted(System.currentTimeMillis(), reqId, i, (i % 1_000_000));

                producer.send(new ProducerRecord<>(topicIn, reqId, payload));
            }
            producer.flush();
            log.info("all {} messages sent to in-topic. waiting for workers to process...", TOTAL_MESSAGES);
        }

        // ожидание завершения обработки воркерами приложения
        boolean finished = receiveLatch.await(600, TimeUnit.MINUTES);
        isRunning.set(false);
        consumerExecutor.shutdown();

        Instant end = Instant.now();
        long elapsedSeconds = Math.max(1, Duration.between(start, end).toSeconds());

        log.info("results");
        log.info("sent to kafka:      {}", TOTAL_MESSAGES);
        log.info("received responses: {}", consumedCount.get());
        log.info("elapsed time:       {} sec", elapsedSeconds);
        log.info("throughput:         {} msg/sec", consumedCount.get() / elapsedSeconds);


        // проверка состояния базы данных через Panache Entity
        long completedInDb = AppChangerTrans.count("status", "COMPLETED");
        log.info("total completed transactions in database: {}", completedInDb);

        Assertions.assertTrue(finished, "pipeline did not finish in time");
        Assertions.assertTrue(completedInDb > 0, "database should contain completed records");
    }
}