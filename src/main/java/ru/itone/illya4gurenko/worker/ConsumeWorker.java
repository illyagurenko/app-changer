package ru.itone.illya4gurenko.worker;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dto.AppChangerConfigDto;
import ru.itone.illya4gurenko.kafka.KafkaConsumerFactory;
import ru.itone.illya4gurenko.service.ConsumeService;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumeWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConsumeWorker.class);

    private final AppChangerConfigDto config;
    private final ConsumeService consumeService;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private KafkaConsumer<String, String> consumer;

    public ConsumeWorker(AppChangerConfigDto config) {
        this.config = config;
        this.consumeService = new ConsumeService();
    }

    @Override
    public void run() {
        String topicIn = config.protocolIn();
        log.info("starting consumer worker thread on topic: {}", topicIn);

        this.consumer = KafkaConsumerFactory.createConsumer(null);
        consumer.subscribe(Collections.singletonList(topicIn));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                log.info("polled {} records from topic: {}", records.count(), topicIn);

                for (ConsumerRecord<String, String> record : records) {
                    consumeService.readAndSaveDb(config, record.value(), record.key());
                }

                // коммитим смещение в кafka только когда вся пачка успешно записана в БД
                consumer.commitSync();
                log.debug("batch committed successfully to Kafka offset.");
            }
        } catch (WakeupException e) {
            if (isRunning.get()) {
                log.error("unexpected wakeup in consumer worker", e);
            }
        } catch (Exception e) {
            log.error("Fatal error in consumer worker loop", e);
        } finally {
            closeQuietly();
            log.info("consumer worker thread terminated.");
        }
    }

    public void stop() {
        log.info("stopping consumer worker");
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
                log.error("error closing kafka consumer", e);
            }
        }
    }
}
