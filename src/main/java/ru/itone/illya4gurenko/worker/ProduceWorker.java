package ru.itone.illya4gurenko.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.kafka.KafkaProducerFactory;
import ru.itone.illya4gurenko.service.ProduceService;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * фоновый поток последовательной отправки ответов в кафку
 */
@ApplicationScoped
public class ProduceWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProduceWorker.class);

    @Inject
    ProduceService produceService;

    @Inject
    KafkaProducerFactory producerFactory;

    private AppChangerConfig config;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private KafkaProducer<String, String> producer;

    /**
     * инициализация воркера конфигурацией и создание продюсера
     */
    public synchronized void init(AppChangerConfig config) {
        this.config = config;
        if (this.producer == null) {
            this.producer = producerFactory.createProducer();
        }
    }

    /**
     * основной цикл отправки записей по одной штуке
     */
    @Override
    public void run() {
        log.info("starting produce worker thread on topic: {}", config.protocolOut);

        while (isRunning.get()) {
            try {
                boolean dispatched = produceService.dispatchNext(producer, config.protocolOut);

                if (!dispatched) {
                    Thread.sleep(20);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("error in produce worker loop", e);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.info("produce worker thread terminated");
    }

    /**
     * остановка потока отправки и закрытие продюсера
     */
    public synchronized void stop() {
        isRunning.set(false);
        if (producer != null) {
            try {
                producer.flush();
                producer.close();
            } catch (Exception e) {
                log.error("error closing kafka producer", e);
            }
            producer = null;
        }
    }
}