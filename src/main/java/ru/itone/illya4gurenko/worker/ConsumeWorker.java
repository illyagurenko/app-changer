package ru.itone.illya4gurenko.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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

@ApplicationScoped
public class ConsumeWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConsumeWorker.class);

    @Inject
    ConsumeService consumeService;

    @Inject
    KafkaConsumerFactory consumerFactory;

    private AppChangerConfigDto config;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private KafkaConsumer<String, String> consumer;

    public void init(AppChangerConfigDto config) {
        this.config = config;
    }

    @Override
    public void run() {
        if (config == null) {
            throw new IllegalStateException("consumeWorker is not initialized! Call init(config) first.");
        }

        String topicIn = config.protocolIn();
        log.info("starting consumeWorker thread on topic: {}", topicIn);

        this.consumer = consumerFactory.createConsumer(null);
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

                // коммитим смещение в Kafka только когда вся пачка успешно записана в БД
                consumer.commitSync();
                log.debug("batch committed successfully to Kafka offset.");
            }
        } catch (WakeupException e) {
            if (isRunning.get()) {
                log.error("unexpected wakeup in ConsumeWorker", e);
            }
        } catch (Exception e) {
            log.error("fatal error in consumeWorker loop", e);
        } finally {
            closeQuietly();
            log.info("consumeWorker thread terminated.");
        }
    }

    public void stop() {
        log.info("stopping consumeWorker");
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
                log.error("error closing Kafka consumer", e);
            }
        }
    }
}