package ru.itone.illya4gurenko.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.db.DataSourceProvider;
import ru.itone.illya4gurenko.dto.TransRecord;
import ru.itone.illya4gurenko.service.ProcessingService;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessingWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProcessingWorker.class);

    private final TransDao transDao;
    private final ProcessingService processingService;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    public ProcessingWorker() {
        this.transDao = new TransDao();
        this.processingService = new ProcessingService();
    }

    @Override
    public void run() {
        log.info("Starting ProcessingWorker thread...");

        while (isRunning.get()) {
            try {
                List<TransRecord> batch;

                try (Connection conn = DataSourceProvider.getConnection()) {
                    // Выбираем пачку записей со статусом RECEIVED
                    batch = transDao.fetchBatchForProcessing(conn, 10);
                }

                if (batch.isEmpty()) {
                    // Если записей нет, ждем 1 секунду
                    Thread.sleep(1000);
                    continue;
                }

                log.info("Found {} transaction(s) ready for processing", batch.size());

                for (TransRecord record : batch) {
                    if (!isRunning.get()) break;
                    processingService.processTransaction(record);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("ProcessingWorker interrupted.");
                break;
            } catch (Exception e) {
                log.error("Error in ProcessingWorker loop", e);
                try {
                    Thread.sleep(2000); // пауза при сбое БД
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("ProcessingWorker thread terminated.");
    }

    public void stop() {
        log.info("Stopping ProcessingWorker...");
        isRunning.set(false);
    }
}