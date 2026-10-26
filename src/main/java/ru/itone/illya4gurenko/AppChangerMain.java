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
import ru.itone.illya4gurenko.worker.ProduceWorker;
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

    @Inject
    ProduceWorker produceWorker;

    private Thread consumeThread;
    private Thread processingThread;
    private Thread dispatcherThread;

    public static void main(String[] args) {
        Quarkus.run(args);
    }

    void onStart(@Observes StartupEvent ev) {
        log.info("starting app-changer on Quarkus");

        AppChangerConfigDto config = configDao.findBySystemId("GRU");
        if (config == null) {
            log.error("config for SYSTEMID='GRU' not found in database!");
            return;
        }

        log.info("loaded config: SYSTEMID={}, IN={}, OUT={}, STATUS_IN={}, STATUS_OUT={}",
                config.systemId(), config.protocolIn(), config.protocolOut(),
                config.statusIn(), config.statusOut());

        // Поток 1: Consumer (Kafka IN -> TRANS 'RECEIVED')
        if (config.statusIn() == 1) {
            consumeWorker.init(config);
            consumeThread = new Thread(consumeWorker, "consume-worker-thread");
            consumeThread.start();
            log.info("consumeWorker thread started on topic: {}", config.protocolIn());
        }

        // Поток 2: Processor ('RECEIVED' -> Procedure -> 'READY_TO_SEND')
        processingThread = new Thread(processingWorker, "processing-worker-thread");
        processingThread.start();
        log.info("processingWorker thread started.");

        // Поток 3: Dispatcher ('READY_TO_SEND' -> Kafka OUT -> 'COMPLETED')
        if (config.statusOut() == 1) {
            produceWorker.init(config);
            dispatcherThread = new Thread(produceWorker, "produce-worker-thread");
            dispatcherThread.start();
            log.info("produceWorker thread started on topic: {}", config.protocolOut());
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        log.info("stopping app-changer workers");

        if (consumeWorker != null) consumeWorker.stop();
        if (processingWorker != null) processingWorker.stop();
        if (produceWorker != null) produceWorker.stop();

        log.info("all workers stopped. Shutdown complete.");
    }
}