package ru.itone.illya4gurenko;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.itone.illya4gurenko.dao.ConfigDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.entity.AppChangerTrans;
import ru.itone.illya4gurenko.service.ConsumeService;
import ru.itone.illya4gurenko.service.ProcessingService;

import java.util.List;
import java.util.UUID;

/**
 * интеграционный тест сквозного пайплайна транзакции
 */
@QuarkusTest
public class AppChangerFlowTest {

    @Inject
    ConfigDao configDao;

    @Inject
    TransDao transDao;

    @Inject
    ConsumeService consumeService;

    @Inject
    ProcessingService processingService;

    @Test
    @DisplayName("тест обработки сообщения от сохранения до формирования ответа")
    void testEndToEndTransactionProcessing() {
        AppChangerConfig config = configDao.findBySystemId("GRU");
        Assertions.assertNotNull(config, "config for GRU must exist in test db");

        String reqId = "test-" + UUID.randomUUID();
        String jsonPayload = """
            {
              "actualTimestamp": 1729000000000,
              "systemId": "GRU",
              "requestId": "%s",
              "eventType": "BALANCE",
              "entityType": "ACCOUNT",
              "events": [
                {
                  "id": "1001",
                  "systemAccount": "40817810099910004321",
                  "currency": "RUB",
                  "XALFA": 500.50,
                  "operation": "CR"
                }
              ]
            }
            """.formatted(reqId);

        // сохранение во входящие
        consumeService.readAndSaveDb(config, jsonPayload, reqId);

        List<Long> receivedIds = transDao.findIdsByStatus("RECEIVED", 50);
        Assertions.assertFalse(receivedIds.isEmpty());

        Long transId = receivedIds.getFirst();

        // обработка через процедуру
        processingService.processTransaction(transId);

        // проверка перехода в READY_TO_SEND и наличия сформированного JSON
        AppChangerTrans processedTrans = transDao.findByIdLocked(transId);
        Assertions.assertEquals("READY_TO_SEND", processedTrans.status);
        Assertions.assertNotNull(processedTrans.respData);
        Assertions.assertTrue(processedTrans.respData.contains("\"status\":\"SUCCESS\""));
        Assertions.assertTrue(processedTrans.respData.contains("40817810099910004321"));
    }
}