package ru.itone.illya4gurenko.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProduceServiceTest {

    private final TransDao transDao = Mockito.mock(TransDao.class);
    private final IoMsgsDao ioMsgsDao = Mockito.mock(IoMsgsDao.class);
    @SuppressWarnings("unchecked")
    private final KafkaProducer<String, String> producer = Mockito.mock(KafkaProducer.class);

    private ProduceService service;

    @BeforeEach
    void setUp() {
        service = new ProduceService();
        service.transDao = transDao;
        service.ioMsgsDao = ioMsgsDao;
        service.nodeId = "162";
    }

    @Test
    @DisplayName("пропуск отправки при null транзакции")
    void testDispatchTransactionNull() {
        service.dispatchTransaction(null, producer, "t-out");
        verifyNoInteractions(producer);
    }

    @Test
    @DisplayName("успешная отправка транзакции продюсером")
    void testDispatchTransactionSuccess() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 1L;
        trans.requestId = "req-1";
        trans.respData = "{}";

        service.dispatchTransaction(trans, producer, "t-out");

        assertEquals("COMPLETED", trans.status);
    }

    @Test
    @DisplayName("обработка ошибки в колбэке kafka продюсера")
    void testDispatchTransactionCallbackError() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 3L;
        trans.requestId = "req-cb";
        trans.respData = "{}";

        doAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
            if (callback != null) {
                callback.onCompletion(null, new RuntimeException("Delivery failed"));
            }
            return null;
        }).when(producer).send(any(), any());

        service.dispatchTransaction(trans, producer, "t-out");

        assertEquals("COMPLETED", trans.status);
    }

    @Test
    @DisplayName("обработка исключения при синхронном вызове send")
    void testDispatchTransactionSendThrows() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 4L;
        trans.requestId = "req-throw";
        trans.respData = "{}";

        doThrow(new RuntimeException("Immediate send error")).when(producer).send(any(), any());

        service.dispatchTransaction(trans, producer, "t-out");

        assertNull(trans.status);
    }

    @Test
    @DisplayName("отправка следующей транзакции при их отсутствии в очереди")
    void testDispatchNextWhenEmpty() {
        when(transDao.acquireNextForSending()).thenReturn(null);

        assertFalse(service.dispatchNext(producer, "t-out"));
    }

    @Test
    @DisplayName("успешная отправка следующей готовой транзакции")
    void testDispatchNextWhenRecordPresent() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 2L;
        trans.requestId = "req-2";
        trans.respData = "{}";
        when(transDao.acquireNextForSending()).thenReturn(trans);

        assertTrue(service.dispatchNext(producer, "t-out"));
    }
}

