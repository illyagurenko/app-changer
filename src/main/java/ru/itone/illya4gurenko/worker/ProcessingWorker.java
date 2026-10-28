package ru.itone.illya4gurenko.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.service.ProcessingService;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * поток обработки полученных транзакций
 */
@ApplicationScoped
public class ProcessingWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProcessingWorker.class);

    @Inject
    TransDao transDao;

    @Inject
    ProcessingService processingService;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    @Override
    public void run() {
        log.info("starting processing worker thread");

        while (isRunning.get()) {
            try {
                // Вызываем DAO
                List<Long> ids = transDao.findIdsByStatus("RECEIVED", 10);

                if (ids.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }

                for (Long id : ids) {
                    if (!isRunning.get()) break;
                    processingService.processTransaction(id);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("error in processing worker loop", e);
                try { Thread.sleep(2000); } catch (InterruptedException ex) { break; }
            }
        }
        log.info("processing worker thread terminated");
    }

    public void stop() {
        isRunning.set(false);
    }
}