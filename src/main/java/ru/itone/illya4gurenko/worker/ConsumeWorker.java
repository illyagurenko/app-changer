package ru.itone.illya4gurenko.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.kafka.KafkaConsumerFactory;
import ru.itone.illya4gurenko.service.ConsumeService;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * поток чтения данных из кафки
 */
@ApplicationScoped
public class ConsumeWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConsumeWorker.class);

    @Inject
    ConsumeService consumeService;

    @Inject
    KafkaConsumerFactory consumerFactory;

    private AppChangerConfig config;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private KafkaConsumer<String, String> consumer;

    public void init(AppChangerConfig config) {
        this.config = config;
    }

    @Override
    public void run() {
        log.info("starting consume worker thread on topic: {}", config.protocolIn);
        this.consumer = consumerFactory.createConsumer(null);
        consumer.subscribe(Collections.singletonList(config.protocolIn));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) continue;

                log.info("polled {} records from topic", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    consumeService.readAndSaveDb(config, record.value(), record.key());
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            if (isRunning.get()) log.error("unexpected wakeup", e);
        } catch (Exception e) {
            log.error("fatal error in consume worker loop", e);
        } finally {
            if (consumer != null) consumer.close();
            log.info("consume worker thread terminated");
        }
    }

    public void stop() {
        isRunning.set(false);
        if (consumer != null) consumer.wakeup();
    }
}