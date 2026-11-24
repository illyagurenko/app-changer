package ru.itone.illya4gurenko;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.client.AppControllerClient;
import ru.itone.illya4gurenko.client.DispatcherClient;
import ru.itone.illya4gurenko.dao.ProcedureDao;
import ru.itone.illya4gurenko.dto.ProcedureResult;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherDatasourceDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseBodyDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseDataDto;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.entity.AppChangerTrans;
import ru.itone.illya4gurenko.service.ConsumeService;
import ru.itone.illya4gurenko.service.ProcessingService;
import ru.itone.illya4gurenko.service.ProduceService;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

@QuarkusTest
class ServiceIntegrationTest {

    @Inject
    ProcessingService processingService;

    @Inject
    ConsumeService consumeService;

    @Inject
    ProduceService produceService;

    @InjectMock
    AppControllerClient appControllerClient;

    @InjectMock
    DispatcherClient dispatcherClient;

    @InjectMock
    ProcedureDao procedureDao;

    @Test
    @DisplayName("успешная обработка транзакции процессором")
    void testProcessTransactionSuccess() throws Exception {
        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 1L;
        trans.requestId = "req-1";
        trans.reqData = """
                {
                  "requestId": "req-1",
                  "events": [
                    { "id": "10", "systemAccount": "ACC1", "operation": "PLUS" }
                  ]
                }
                """;

        Mockito.when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);

        DispatcherResponseBodyDto dispResp = new DispatcherResponseBodyDto(
                "req-1", "APP_DISPATCHER", "Success", null,
                new DispatcherResponseDataDto(List.of(new DispatcherDatasourceDto("jdbc:h2:mem:changer_test", List.of("ACC1"))))
        );
        Mockito.when(dispatcherClient.sendPost(any())).thenReturn(dispResp);

        Mockito.when(procedureDao.executeOnConnection(any(), any()))
                .thenReturn(new ProcedureResult(0, "OK", Map.of("ACC1", new ProcedureResult.CalculatedBalance(BigDecimal.valueOf(100), BigDecimal.valueOf(200)))));

        processingService.processTransaction(trans);

        assertEquals("READY_TO_SEND", trans.status);
    }

    @Test
    @DisplayName("обработка ошибки процедуры процессором")
    void testProcessTransactionError() throws Exception {
        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 2L;
        trans.requestId = "req-2";
        trans.reqData = """
                {
                  "requestId": "req-2",
                  "events": [
                    { "id": "20", "systemAccount": "ACC2", "operation": "PLUS" }
                  ]
                }
                """;

        Mockito.when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);

        DispatcherResponseBodyDto dispResp = new DispatcherResponseBodyDto(
                "req-2", "APP_DISPATCHER", "Success", null,
                new DispatcherResponseDataDto(List.of(new DispatcherDatasourceDto("jdbc:h2:mem:changer_test", List.of("ACC2"))))
        );
        Mockito.when(dispatcherClient.sendPost(any())).thenReturn(dispResp);

        Mockito.when(procedureDao.executeOnConnection(any(), any()))
                .thenReturn(new ProcedureResult(-1, "Procedure error", Map.of()));

        processingService.processTransaction(trans);

        assertEquals("0", trans.respCode);
        assertEquals("READY_TO_SEND", trans.status);
    }

    @Test
    @Transactional
    @DisplayName("успешное сохранение входящего сообщения консьюмером")
    void testConsumeSuccess() {
        AppChangerConfig config = new AppChangerConfig();
        config.protocolType = "JSON";
        config.systemId = "GRU";
        config.eventType = "BALANCE";

        String json = "{\"requestId\":\"req-it-cons\",\"events\":[{\"id\":\"1\",\"systemAccount\":\"ACC1\"}]}";
        consumeService.readAndSaveDb(config, json, "key-it-cons");

        assertNotNull(AppChangerTrans.find("requestId", "req-it-cons").firstResult());
    }

    @Test
    @Transactional
    @DisplayName("пропуск пустого сообщения консьюмером")
    void testConsumeEmpty() {
        AppChangerConfig config = new AppChangerConfig();
        consumeService.readAndSaveDb(config, "", "key-it-empty");

        assertEquals(0, AppChangerTrans.count("requestId", "key-it-empty"));
    }

    @Test
    @DisplayName("успешная отправка готового ответа продюсером")
    void testProduceSuccess() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 50L;
        trans.requestId = "req-it-prod";
        trans.respData = "{}";

        @SuppressWarnings("unchecked")
        KafkaProducer<String, String> producer = Mockito.mock(KafkaProducer.class);
        produceService.dispatchTransaction(trans, producer, "t-out");

        assertEquals("COMPLETED", trans.status);
    }

    @Test
    @DisplayName("пропуск отправки при отсутствии записей продюсером")
    void testProduceWhenNoRecords() {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, String> producer = Mockito.mock(KafkaProducer.class);
        boolean sent = produceService.dispatchNext(producer, "t-out");

        assertFalse(sent);
    }
}
