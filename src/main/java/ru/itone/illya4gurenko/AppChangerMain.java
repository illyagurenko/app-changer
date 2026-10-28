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
import ru.itone.illya4gurenko.worker.ConsumeWorker;
import ru.itone.illya4gurenko.worker.ProcessingWorker;
import ru.itone.illya4gurenko.worker.ProduceWorker;

/**
 * главная точка входа приложения
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

    public static void main(String[] args) {
        Quarkus.run(args);
    }

    void onStart(@Observes StartupEvent ev) {
        log.info("starting app-changer on quarkus");

        AppChangerConfig config = configDao.findBySystemId("GRU");
        if (config == null) {
            log.error("config for systemid='GRU' not found in database");
            return;
        }

        log.info("loaded config: in={}, out={}, status_in={}, status_out={}",
                config.protocolIn, config.protocolOut, config.statusIn, config.statusOut);

        if (config.statusIn != null && config.statusIn == 1) {
            consumeWorker.init(config);
            new Thread(consumeWorker, "consume-worker-thread").start();
        }

        new Thread(processingWorker, "processing-worker-thread").start();

        if (config.statusOut != null && config.statusOut == 1) {
            produceWorker.init(config);
            new Thread(produceWorker, "produce-worker-thread").start();
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        log.info("stopping app-changer workers");
        consumeWorker.stop();
        processingWorker.stop();
        produceWorker.stop();
        log.info("shutdown complete");
    }
}