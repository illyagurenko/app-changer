package ru.itone.illya4gurenko.worker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dto.AppChangerConfigDto;
import ru.itone.illya4gurenko.kafka.KafkaConsumerFactory;
import ru.itone.illya4gurenko.service.IngestionService;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class IngestionWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(IngestionWorker.class);

    private final AppChangerConfigDto config;
    private final IngestionService ingestionService;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private KafkaConsumer<String, String> consumer;

    public IngestionWorker(AppChangerConfigDto config) {
        this.config = config;
        this.ingestionService = new IngestionService();
    }

    @Override
    public void run() {
        String topicIn = config.protocolIn();
        log.info("Starting IngestionWorker thread on topic: {}", topicIn);

        this.consumer = KafkaConsumerFactory.createConsumer(null);
        consumer.subscribe(Collections.singletonList(topicIn));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                log.info("Polled {} records from topic: {}", records.count(), topicIn);

                for (ConsumerRecord<String, String> record : records) {
                    ingestionService.processIncomingPayload(config, record.value(), record.key());
                }

                // Коммитим смещение в Kafka только когда ВСЯ пачка успешно записана в БД
                consumer.commitSync();
                log.debug("Batch committed successfully to Kafka offset.");
            }
        } catch (WakeupException e) {
            if (isRunning.get()) {
                log.error("Unexpected wakeup in IngestionWorker", e);
            }
        } catch (Exception e) {
            log.error("Fatal error in IngestionWorker loop", e);
        } finally {
            closeQuietly();
            log.info("IngestionWorker thread terminated.");
        }
    }

    public void stop() {
        log.info("Stopping IngestionWorker...");
        isRunning.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void closeQuietly() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.error("Error closing Kafka consumer", e);
            }
        }
    }
}
