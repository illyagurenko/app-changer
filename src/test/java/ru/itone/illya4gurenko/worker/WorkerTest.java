package ru.itone.illya4gurenko.worker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.kafka.KafkaProducerFactory;
import ru.itone.illya4gurenko.service.ProcessingService;
import ru.itone.illya4gurenko.service.ProduceService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;

class WorkerTest {

    @Test
    @DisplayName("остановка и запуск воркера обработки")
    void testProcessingWorkerStopAndRun() {
        ProcessingWorker worker = new ProcessingWorker();
        worker.processingService = Mockito.mock(ProcessingService.class);
        worker.stop();
        assertDoesNotThrow(worker::run);
    }

    @Test
    @DisplayName("одиночный цикл обработки воркера обработки")
    void testProcessingWorkerRunOnce() {
        ProcessingWorker worker = new ProcessingWorker();
        ProcessingService svc = Mockito.mock(ProcessingService.class);
        worker.processingService = svc;
        Mockito.when(svc.processNext()).thenAnswer(inv -> {
            worker.stop();
            return true;
        });

        assertDoesNotThrow(worker::run);
    }

    @Test
    @DisplayName("инициализация и остановка воркера консьюмера")
    void testConsumeWorkerInitAndStop() {
        ConsumeWorker worker = new ConsumeWorker();
        AppChangerConfig config = new AppChangerConfig();
        config.protocolIn = "p-in";
        worker.init(config);
        assertDoesNotThrow(worker::stop);
    }

    @Test
    @DisplayName("инициализация и остановка воркера продюсера")
    void testProduceWorkerInitAndStop() {
        ProduceWorker worker = new ProduceWorker();
        worker.produceService = Mockito.mock(ProduceService.class);
        worker.producerFactory = Mockito.mock(KafkaProducerFactory.class);
        AppChangerConfig config = new AppChangerConfig();
        config.protocolOut = "p-out";

        worker.init(config);
        worker.stop();
        assertDoesNotThrow(worker::run);
    }

    @Test
    @DisplayName("одиночный цикл отправки воркера продюсера")
    void testProduceWorkerRunOnce() {
        ProduceWorker worker = new ProduceWorker();
        ProduceService svc = Mockito.mock(ProduceService.class);
        worker.produceService = svc;
        worker.producerFactory = Mockito.mock(KafkaProducerFactory.class);
        AppChangerConfig config = new AppChangerConfig();
        config.protocolOut = "p-out";
        worker.init(config);

        Mockito.when(svc.dispatchNext(any(), any())).thenAnswer(inv -> {
            worker.stop();
            return true;
        });

        assertDoesNotThrow(worker::run);
    }
}
