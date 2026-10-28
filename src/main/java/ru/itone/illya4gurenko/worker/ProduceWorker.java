package ru.itone.illya4gurenko.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.entity.AppChangerTrans;
import ru.itone.illya4gurenko.kafka.KafkaProducerFactory;
import ru.itone.illya4gurenko.service.ProduceService;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * поток отправки обработанных сообщений наружу
 */
@ApplicationScoped
public class ProduceWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProduceWorker.class);

    @Inject
    TransDao transDao;

    @Inject
    ProduceService produceService;

    @Inject
    KafkaProducerFactory producerFactory;

    private AppChangerConfig config;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private KafkaProducer<String, String> producer;

    public void init(AppChangerConfig config) {
        this.config = config;
    }

    @Override
    public void run() {
        log.info("starting produce worker thread on topic: {}", config.protocolOut);
        this.producer = producerFactory.createProducer();

        while (isRunning.get()) {
            try {
                List<Long> ids = transDao.findIdsByStatus("READY_TO_SEND", 10);

                if (ids.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }

                for (Long id : ids) {
                    if (!isRunning.get()) break;
                    produceService.dispatchTransaction(id, producer, config.protocolOut);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("error in produce worker loop", e);
                try { Thread.sleep(2000); } catch (InterruptedException ex) { break; }
            }
        }
        if (producer != null) producer.close();
        log.info("produce worker thread terminated");
    }

    public void stop() {
        isRunning.set(false);
    }
}