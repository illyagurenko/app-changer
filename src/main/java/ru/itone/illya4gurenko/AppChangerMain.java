package ru.itone.illya4gurenko;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.ConfigDao;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.kafka.KafkaTopicManager;
import ru.itone.illya4gurenko.worker.ConsumeWorker;
import ru.itone.illya4gurenko.worker.ProcessingWorker;
import ru.itone.illya4gurenko.worker.ProduceWorker;

/**
 * главная точка входа приложения и управление жизненным циклом потоков
 */
@QuarkusMain
@ApplicationScoped
public class AppChangerMain {
    private static final Logger log = LoggerFactory.getLogger(AppChangerMain.class);

    @Inject
    ConfigDao configDao;

    @Inject
    ConsumeWorker consumeWorker;

    @Inject
    ProcessingWorker processingWorker;

    @Inject
    ProduceWorker produceWorker;

    @Inject
    KafkaTopicManager kafkaTopicManager;

    public static void main(String[] args) {
        Quarkus.run(args);
    }

    /**
     * запуск воркеров при старте приложения
     */
    void onStart(@Observes StartupEvent ev) {
        log.info("starting app-changer on quarkus");

        AppChangerConfig config = configDao.findBySystemId("GRU");
        if (config == null) {
            log.error("config for systemid='GRU' not found in database");
            return;
        }

        kafkaTopicManager.ensureTopicPartitions(config.protocolIn, 4);
        kafkaTopicManager.ensureTopicPartitions(config.protocolOut, 4);

        log.info("loaded config: in={}, out={}, status_in={}, status_out={}",
                config.protocolIn, config.protocolOut, config.statusIn, config.statusOut);

        if (config.statusIn != null && config.statusIn == 1) {
            consumeWorker.init(config);
            new Thread(consumeWorker, "consume-worker-thread").start();
        }


        for (int i = 0; i < 1; i++) {
            new Thread(processingWorker, "processing-worker-" + i).start();
        }
        
        if (config.statusOut != null && config.statusOut == 1) {
            produceWorker.init(config);
            for (int i = 0; i < 1; i++) {
                new Thread(produceWorker, "produce-worker-" + i).start();
            }
        }
    }

    /**
     * корректная остановка воркеров при завершении работы приложения
     */
    void onStop(@Observes ShutdownEvent ev) {
        log.info("stopping app-changer workers");
        consumeWorker.stop();
        processingWorker.stop();
        produceWorker.stop();
        log.info("shutdown complete");
    }
}