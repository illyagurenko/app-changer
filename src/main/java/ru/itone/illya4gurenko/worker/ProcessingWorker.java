package ru.itone.illya4gurenko.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.TransRecord;
import ru.itone.illya4gurenko.service.ProcessingService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class ProcessingWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProcessingWorker.class);

    @Inject
    DataSource dataSource;

    @Inject
    TransDao transDao;

    @Inject
    ProcessingService processingService;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    @Override
    public void run() {
        log.info("starting ProcessingWorker thread");

        while (isRunning.get()) {
            try {
                List<TransRecord> batch;

                try (Connection conn = dataSource.getConnection()) {
                    // Выбираем пачку записей со статусом RECEIVED
                    batch = transDao.fetchBatchForProcessing(conn, 10);
                }

                if (batch.isEmpty()) {
                    // если записей нет, ждем 1 секунду
                    Thread.sleep(1000);
                    continue;
                }

                log.info("found {} transaction(s) ready for processing", batch.size());

                for (TransRecord record : batch) {
                    if (!isRunning.get()) break;
                    processingService.processTransaction(record);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("pProcessingWorker interrupted.");
                break;
            } catch (Exception e) {
                log.error("error in processingWorker loop", e);
                try {
                    Thread.sleep(2000); // пауза при сбое БД
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("processingWorker thread terminated.");
    }

    public void stop() {
        log.info("stopping processingWorker");
        isRunning.set(false);
    }
}