package ru.itone.illya4gurenko.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.client.AppControllerClient;
import ru.itone.illya4gurenko.client.DispatcherClient;
import ru.itone.illya4gurenko.dao.ProcedureDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.ProcedureResult;
import ru.itone.illya4gurenko.dto.ResponseEventDto;
import ru.itone.illya4gurenko.dto.ResponseKafkaDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherDatasourceDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseBodyDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseDataDto;
import ru.itone.illya4gurenko.entity.AppChangerTrans;
import ru.itone.illya4gurenko.util.JsonUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class ProcessingServiceTest {

    private final TransDao transDao = Mockito.mock(TransDao.class);
    private final ProcedureDao procedureDao = Mockito.mock(ProcedureDao.class);
    private final AppControllerClient appControllerClient = Mockito.mock(AppControllerClient.class);
    private final DispatcherClient dispatcherClient = Mockito.mock(DispatcherClient.class);
    private final DynamicDataSourceService dynamicDataSourceService = Mockito.mock(DynamicDataSourceService.class);

    private ProcessingService service;

    @BeforeEach
    void setUp() {
        service = new ProcessingService();
        service.transDao = transDao;
        service.procedureDao = procedureDao;
        service.appControllerClient = appControllerClient;
        service.dispatcherClient = dispatcherClient;
        service.dynamicDataSourceService = dynamicDataSourceService;
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
    @DisplayName("обработка ошибки вызова app-dispatcher")
    void testProcessTransactionDispatcherCallFailed() throws Exception {
        AppChangerTrans trans = new AppChangerTrans();
        trans.requestId = "r-disp-err";
        trans.reqData = "{\"requestId\":\"r-disp-err\",\"events\":[{\"systemAccount\":\"ACC1\",\"id\":\"1\"}]}";

        when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);
        when(dispatcherClient.sendPost(any())).thenThrow(new IOException("Connection reset"));

        service.processTransaction(trans);

        assertEquals("ROUTING_ERR", trans.respCode);
        assertTrue(trans.respDesc.contains("app_dispatcher call failed"));
    }

    @Test
    @DisplayName("успешная обработка транзакции с получением датасорса от диспетчера")
    void testProcessTransactionSuccess() throws Exception {
        AppChangerTrans trans = new AppChangerTrans();
        trans.requestId = "r-ok";
        trans.reqData = "{\"requestId\":\"r-ok\",\"events\":[{\"systemAccount\":\"ACC1\",\"id\":\"1\",\"operation\":\"PLUS\"}]}";

        when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);

        DispatcherResponseBodyDto dispResp = new DispatcherResponseBodyDto(
                "r-ok", "APP_DISPATCHER", "Success", null,
                new DispatcherResponseDataDto(List.of(new DispatcherDatasourceDto("jdbc:postgresql://localhost:5432/db", List.of("ACC1"))))
        );
        when(dispatcherClient.sendPost(any())).thenReturn(dispResp);

        Connection mockConn = Mockito.mock(Connection.class);
        when(dynamicDataSourceService.getConnection("jdbc:postgresql://localhost:5432/db")).thenReturn(mockConn);
        when(procedureDao.executeOnConnection(any(), any()))
                .thenReturn(new ProcedureResult(0, "OK", Map.of("ACC1", new ProcedureResult.CalculatedBalance(BigDecimal.ONE, BigDecimal.TEN))));

        service.processTransaction(trans);

        assertEquals("0", trans.respCode);
        assertEquals("READY_TO_SEND", trans.status);

        ResponseKafkaDto respDto = JsonUtils.fromJson(trans.respData, ResponseKafkaDto.class);
        assertNotNull(respDto);
        assertEquals("SUCCESS", respDto.events().get(0).status());
        assertEquals(BigDecimal.TEN, respDto.events().get(0).data().newTbal());
    }

    @Test
    @DisplayName("обработка пустого jdbc_url от диспетчера -> ошибка EMPTY_JDBC_URL")
    void testProcessTransactionEmptyJdbcUrl() throws Exception {
        AppChangerTrans trans = new AppChangerTrans();
        trans.requestId = "r-empty-url";
        trans.reqData = "{\"requestId\":\"r-empty-url\",\"events\":[{\"systemAccount\":\"ACC_NO_URL\",\"id\":\"2\"}]}";

        when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);

        DispatcherResponseBodyDto dispResp = new DispatcherResponseBodyDto(
                "r-empty-url", "APP_DISPATCHER", "Success", null,
                new DispatcherResponseDataDto(List.of(new DispatcherDatasourceDto("", List.of("ACC_NO_URL"))))
        );
        when(dispatcherClient.sendPost(any())).thenReturn(dispResp);

        service.processTransaction(trans);

        assertEquals("0", trans.respCode);
        assertEquals("ALL_EVENTS_ERROR", trans.respDesc);

        ResponseKafkaDto respDto = JsonUtils.fromJson(trans.respData, ResponseKafkaDto.class);
        ResponseEventDto event = respDto.events().get(0);
        assertEquals("ERROR", event.status());
        assertEquals("EMPTY_JDBC_URL", event.error().code());
    }

    @Test
    @DisplayName("обработка недоступной базы данных -> ошибка DB_UNREACHABLE")
    void testProcessTransactionDbUnreachable() throws Exception {
        AppChangerTrans trans = new AppChangerTrans();
        trans.requestId = "r-unreach";
        trans.reqData = "{\"requestId\":\"r-unreach\",\"events\":[{\"systemAccount\":\"ACC_FAIL\",\"id\":\"3\"}]}";

        when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);

        DispatcherResponseBodyDto dispResp = new DispatcherResponseBodyDto(
                "r-unreach", "APP_DISPATCHER", "Success", null,
                new DispatcherResponseDataDto(List.of(new DispatcherDatasourceDto("jdbc:postgresql://bad-host:5432/bad_db", List.of("ACC_FAIL"))))
        );
        when(dispatcherClient.sendPost(any())).thenReturn(dispResp);

        when(dynamicDataSourceService.getConnection("jdbc:postgresql://bad-host:5432/bad_db"))
                .thenThrow(new SQLException("Connection refused"));

        service.processTransaction(trans);

        assertEquals("0", trans.respCode);
        assertEquals("ALL_EVENTS_ERROR", trans.respDesc);

        ResponseKafkaDto respDto = JsonUtils.fromJson(trans.respData, ResponseKafkaDto.class);
        ResponseEventDto event = respDto.events().get(0);
        assertEquals("ERROR", event.status());
        assertEquals("DB_UNREACHABLE", event.error().code());
    }

    @Test
    @DisplayName("обработка ошибки выполнения хранимой процедуры")
    void testProcessTransactionProcedureFailure() throws Exception {
        AppChangerTrans trans = new AppChangerTrans();
        trans.requestId = "r-fail";
        trans.reqData = "{\"requestId\":\"r-fail\",\"events\":[{\"systemAccount\":\"ACC1\",\"id\":\"1\"}]}";

        when(appControllerClient.checkAndRouteAccounts(any())).thenReturn(true);

        DispatcherResponseBodyDto dispResp = new DispatcherResponseBodyDto(
                "r-fail", "APP_DISPATCHER", "Success", null,
                new DispatcherResponseDataDto(List.of(new DispatcherDatasourceDto("jdbc:postgresql://localhost:5432/db", List.of("ACC1"))))
        );
        when(dispatcherClient.sendPost(any())).thenReturn(dispResp);

        Connection mockConn = Mockito.mock(Connection.class);
        when(dynamicDataSourceService.getConnection(anyString())).thenReturn(mockConn);
        when(procedureDao.executeOnConnection(any(), any()))
                .thenReturn(new ProcedureResult(500, "Proc error", Map.of()));

        service.processTransaction(trans);

        ResponseKafkaDto respDto = JsonUtils.fromJson(trans.respData, ResponseKafkaDto.class);
        assertEquals("ERROR", respDto.events().get(0).status());
        assertEquals("500", respDto.events().get(0).error().code());
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
