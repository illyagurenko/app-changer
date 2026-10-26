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
import ru.itone.illya4gurenko.dto.AppChangerConfigDto;
import ru.itone.illya4gurenko.worker.ConsumeWorker;
import ru.itone.illya4gurenko.worker.ProcessingWorker;

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

    private Thread consumeThread;
    private Thread processingThread;

    public static void main(String[] args) {
        // запуск среды Quarkus
        Quarkus.run(args);
    }

    void onStart(@Observes StartupEvent ev) {
        log.info("starting app-changer on Quarkus");

        // читаем конфигурацию из БД через ConfigDao
        AppChangerConfigDto config = configDao.findBySystemId("GRU");
        if (config == null) {
            log.error("config for SYSTEMID='GRU' not found in database!");
            return;
        }

        log.info("loaded config: SYSTEMID={}, IN={}, OUT={}, STATUS_IN={}, STATUS_OUT={}",
                config.systemId(), config.protocolIn(), config.protocolOut(),
                config.statusIn(), config.statusOut());

        //  Запуск Потока 1 (ConsumeWorker)
        if (config.statusIn() == 1) {
            consumeWorker.init(config);
            consumeThread = new Thread(consumeWorker, "consume-worker-thread");
            consumeThread.start();
            log.info("consumeWorker thread started on topic: {}", config.protocolIn());
        } else {
            log.warn("ingestion is disabled (STATUS_IN=0)");
        }

        // Запуск Потока 2 (ProcessingWorker)
        processingThread = new Thread(processingWorker, "processing-worker-thread");
        processingThread.start();
        log.info("processingWorker thread started.");
    }

    void onStop(@Observes ShutdownEvent ev) {
        log.info("stopping app-changer workers");

        if (consumeWorker != null) {
            consumeWorker.stop();
        }
        if (processingWorker != null) {
            processingWorker.stop();
        }

        log.info("workers stopped. Shutdown complete.");
    }
}