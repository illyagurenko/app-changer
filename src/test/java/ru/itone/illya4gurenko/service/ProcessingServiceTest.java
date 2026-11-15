package ru.itone.illya4gurenko.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.dao.ProcedureDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.ProcedureResult;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class ProcessingServiceTest {

    private final TransDao transDao = Mockito.mock(TransDao.class);
    private final ProcedureDao procedureDao = Mockito.mock(ProcedureDao.class);
    private final AppControllerClient appControllerClient = Mockito.mock(AppControllerClient.class);

    private ProcessingService service;

    @BeforeEach
    void setUp() {
        service = new ProcessingService();
        service.transDao = transDao;
        service.procedureDao = procedureDao;
        service.appControllerClient = appControllerClient;
    }

    @Test
    @DisplayName("обработка null транзакции")
    void testProcessTransactionNull() {
        assertDoesNotThrow(() -> service.processTransaction((AppChangerTrans) null));
    }

    @Test
    @DisplayName("обработка транзакции с пустыми данными запроса")
    void testProcessTransactionBlankReqData() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.reqData = "";

        service.processTransaction(trans);

        assertEquals("PROCESSING_ERROR", trans.status);
    }

    @Test
    @DisplayName("обработка отклонения маршрутизации контроллером")
    void testProcessTransactionRoutingRejected() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.requestId = "r-route";
        trans.reqData = "{\"requestId\":\"r-route\",\"events\":[{\"systemAccount\":\"ACC1\",\"id\":1}]}";

        when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(false);

        service.processTransaction(trans);

        assertEquals("ROUTING_ERR", trans.respCode);
    }

    @Test
    @DisplayName("успешная обработка транзакции процессором")
    void testProcessTransactionSuccess() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.requestId = "r-ok";
        trans.reqData = "{\"requestId\":\"r-ok\",\"events\":[{\"systemAccount\":\"ACC1\",\"id\":1,\"operation\":\"PLUS\"}]}";

        when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);
        when(procedureDao.executeChangerDataAccount(any()))
                .thenReturn(new ProcedureResult(0, "OK", Map.of("ACC1", new ProcedureResult.CalculatedBalance(BigDecimal.ONE, BigDecimal.TEN))));

        service.processTransaction(trans);

        assertEquals("0", trans.respCode);
    }

    @Test
    @DisplayName("обработка ошибки выполнения хранимой процедуры")
    void testProcessTransactionProcedureFailure() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.requestId = "r-fail";
        trans.reqData = "{\"requestId\":\"r-fail\",\"events\":[{\"systemAccount\":\"ACC1\",\"id\":1}]}";

        when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);
        when(procedureDao.executeChangerDataAccount(any()))
                .thenReturn(new ProcedureResult(500, "Proc error", Map.of()));

        service.processTransaction(trans);

        assertEquals("500", trans.respCode);
    }

    @Test
    @DisplayName("попытка обработки следующей транзакции при их отсутствии")
    void testProcessNextWhenNoRecords() {
        when(transDao.acquireNextForProcessing()).thenReturn(null);

        assertFalse(service.processNext());
    }

    @Test
    @DisplayName("успешный запуск обработки следующей имеющейся транзакции")
    void testProcessNextWhenRecordPresent() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.reqData = "";
        when(transDao.acquireNextForProcessing()).thenReturn(trans);

        assertTrue(service.processNext());
    }

    @Test
    @DisplayName("обработка транзакции по несуществующему id")
    void testProcessTransactionByIdNull() {
        when(transDao.findById(999L)).thenReturn(null);

        assertDoesNotThrow(() -> service.processTransaction(999L));
    }

    @Test
    @DisplayName("обработка транзакции по существующему id")
    void testProcessTransactionByIdFound() {
        AppChangerTrans trans = new AppChangerTrans();
        trans.reqData = "";
        when(transDao.findById(10L)).thenReturn(trans);

        service.processTransaction(10L);

        assertEquals("PROCESSING_ERROR", trans.status);
    }
}

