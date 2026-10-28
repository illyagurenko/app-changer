package ru.itone.illya4gurenko.load;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * нагрузочный тест генерации 1-2 млн сообщений в кафку
 */
public class AppChangerLoadTest {
    private static final Logger log = LoggerFactory.getLogger(AppChangerLoadTest.class);

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    private static final String TOPIC_IN = System.getProperty("kafka.topic.in", "gru.balance.request");

    // количество сообщений
    private static final int TOTAL_MESSAGES = Integer.getInteger("load.count", 1_000_000);

    @Test
    @Disabled("нагрузочное тестирование при поднятой кафке и бд")
    void runHeavyLoadTest() throws InterruptedException {
        log.info("starting load generator: target={} messages, bootstrap={}", TOTAL_MESSAGES, BOOTSTRAP_SERVERS);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072); // 128 KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 10);

        CountDownLatch latch = new CountDownLatch(TOTAL_MESSAGES);
        AtomicLong errorCount = new AtomicLong(0);

        Instant start = Instant.now();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                String reqId = "load-req-" + i;
                String payload = """
                    {"actualTimestamp":%d,"systemId":"GRU","requestId":"%s","eventType":"BALANCE","entityType":"ACCOUNT","events":[{"id":"%d","systemAccount":"408178100999%08d","currency":"RUB","XALFA":150.00,"operation":"CR"}]}
                    """.formatted(System.currentTimeMillis(), reqId, i, (i % 1000000));

                producer.send(new ProducerRecord<>(TOPIC_IN, reqId, payload), (metadata, exception) -> {
                    if (exception != null) {
                        errorCount.incrementAndGet();
                    }
                    latch.countDown();
                });

                if (i % 100_000 == 0) {
                    log.info("pushed {} of {} messages to kafka...", i, TOTAL_MESSAGES);
                }
            }

            producer.flush();
            log.info("all messages queued, waiting for broker ack...");
            boolean finished = latch.await(10, TimeUnit.MINUTES);

            Instant end = Instant.now();
            long elapsedSeconds = Math.max(1, Duration.between(start, end).toSeconds());
            long rps = TOTAL_MESSAGES / elapsedSeconds;

            log.info("load generation completed in {} seconds. overall rps: {}, failed messages: {}",
                    elapsedSeconds, rps, errorCount.get());

            if (!finished) {
                log.warn("timeout occurred before all message acks were received");
            }
        }
    }
}