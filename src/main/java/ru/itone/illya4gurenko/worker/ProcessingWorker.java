package ru.itone.illya4gurenko.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.service.ProcessingService;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * фоновый поток последовательной обработки транзакций
 */
@ApplicationScoped
public class ProcessingWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProcessingWorker.class);

    @Inject
    ProcessingService processingService;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * основной цикл обработки записей по одной штуке
     */
    @Override
    public void run() {
        log.info("starting processing worker thread");

        while (isRunning.get()) {
            try {
                boolean processed = processingService.processNext();

                if (!processed) {
                    Thread.sleep(20);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("error in processing worker loop", e);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.info("processing worker thread terminated");
    }

    /**
     * остановка потока обработки
     */
    public void stop() {
        isRunning.set(false);
    }
}