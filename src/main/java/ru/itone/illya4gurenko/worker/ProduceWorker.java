package ru.itone.illya4gurenko.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.AppChangerConfigDto;
import ru.itone.illya4gurenko.dto.ReadyToSendRecord;
import ru.itone.illya4gurenko.kafka.KafkaProducerFactory;
import ru.itone.illya4gurenko.service.ProduceService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class ProduceWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProduceWorker.class);

    @Inject
    DataSource dataSource;

    @Inject
    TransDao transDao;

    @Inject
    ProduceService dispatcherService;

    @Inject
    KafkaProducerFactory producerFactory;

    private AppChangerConfigDto config;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private KafkaProducer<String, String> producer;

    public void init(AppChangerConfigDto config) {
        this.config = config;
    }

    @Override
    public void run() {
        if (config == null) {
            throw new IllegalStateException("produceWorker is not initialized! Call init(config) first.");
        }

        String topicOut = config.protocolOut();
        log.info("starting produceWorker thread on topic: {}", topicOut);

        this.producer = producerFactory.createProducer();

        while (isRunning.get()) {
            try {
                List<ReadyToSendRecord> batch;

                try (Connection conn = dataSource.getConnection()) {
                    batch = transDao.fetchBatchForSending(conn, 10);
                }

                if (batch.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }

                log.info("found {} transaction(s) ready to send", batch.size());

                for (ReadyToSendRecord record : batch) {
                    if (!isRunning.get()) break;
                    dispatcherService.dispatch(producer, topicOut, record);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("produceWorker interrupted.");
                break;
            } catch (Exception e) {
                log.error("error in produceWorker loop", e);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        closeQuietly();
        log.info("produceWorker thread terminated.");
    }

    public void stop() {
        log.info("stopping produceWorker");
        isRunning.set(false);
    }

    private void closeQuietly() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close();
            } catch (Exception e) {
                log.error("error closing Kafka producer", e);
            }
        }
    }
}