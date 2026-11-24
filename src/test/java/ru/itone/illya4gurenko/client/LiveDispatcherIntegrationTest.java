package ru.itone.illya4gurenko.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.itone.illya4gurenko.config.AppConfig;
import ru.itone.illya4gurenko.dao.ProcedureDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.ResponseEventDto;
import ru.itone.illya4gurenko.dto.ResponseKafkaDto;
import ru.itone.illya4gurenko.dto.dispatcher.request.DispatcherRequestBodyDto;
import ru.itone.illya4gurenko.dto.dispatcher.request.DispatcherRequestDataDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherDatasourceDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseBodyDto;
import ru.itone.illya4gurenko.entity.AppChangerTrans;
import ru.itone.illya4gurenko.security.AESCryptoService;
import ru.itone.illya4gurenko.service.DynamicDataSourceService;
import ru.itone.illya4gurenko.service.ProcessingService;
import ru.itone.illya4gurenko.util.JsonUtils;

import java.net.http.HttpClient;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Интеграционный тест: взаимодействие app-changer с живым app-dispatcher")
class LiveDispatcherIntegrationTest {

    @BeforeEach
    void ensureDispatcherIsRunning() {
        boolean running = false;
        try (java.net.Socket socket = new java.net.Socket("localhost", 8085)) {
            running = true;
        } catch (Exception ignored) {
        }
        Assumptions.assumeTrue(running, "app-dispatcher не запущен на localhost:8085, пропускаем live-тест");
    }

    private AppConfig createLiveAppConfig() throws Exception {
        AppConfig appConfig = new AppConfig();
        var field = AppConfig.class.getDeclaredField("dispatcherUrl");
        field.setAccessible(true);
        field.set(appConfig, "http://localhost:8085/get-datasoure-by-account");

        var userField = AppConfig.class.getDeclaredField("dispatcherUsername");
        userField.setAccessible(true);
        userField.set(appConfig, "APP_CHANGER");

        var passField = AppConfig.class.getDeclaredField("dispatcherPassword");
        passField.setAccessible(true);
        passField.set(appConfig, "xP7Llfm/e2eItNeGlxNYnQ==");

        var cryptoField = AppConfig.class.getDeclaredField("cryptoService");
        cryptoField.setAccessible(true);
        cryptoField.set(appConfig, AESCryptoService.getInstance());
        return appConfig;
    }

    @Test
    @DisplayName("Реальный HTTP-запрос от клиента app-changer к живому app-dispatcher на порту 8085")
    void testRealCallToLiveDispatcher() throws Exception {
        AppConfig appConfig = createLiveAppConfig();
        ObjectMapper mapper = new ObjectMapper();
        DispatcherClient client = new DispatcherClient(appConfig, mapper, HttpClient.newHttpClient());

        String realRequestId = UUID.randomUUID().toString();
        DispatcherRequestBodyDto request = new DispatcherRequestBodyDto(
                new DispatcherRequestDataDto("APP_CHANGER", realRequestId, List.of("ACC_GPB_001", "ACC_GPB_002", "ACC_GPB_003", "ACC_UNMAPPED_999")),
                "Success",
                null
        );

        DispatcherResponseBodyDto response = client.sendPost(request);

        assertNotNull(response);
        assertEquals(realRequestId, response.requestId());
        assertEquals("APP_DISPATCHER", response.systemId());
        assertEquals("Success", response.status());
        assertNull(response.error());
        assertNotNull(response.data());
        assertNotNull(response.data().datasource());
        assertFalse(response.data().datasource().isEmpty());

        System.out.println("=== УСПЕШНЫЙ ОТВЕТ ОТ ЖИВОГО ДИСПЕТЧЕРА ===");
        for (DispatcherDatasourceDto ds : response.data().datasource()) {
            System.out.println("  jdbc_url: [" + ds.jdbcUrl() + "] -> accounts: " + ds.account());
        }

        DispatcherDatasourceDto group1 = response.data().datasource().stream()
                .filter(d -> "jdbc:postgresql://localhost:5432/changer_db".equals(d.jdbcUrl()))
                .findFirst()
                .orElse(null);
        assertNotNull(group1);
        assertTrue(group1.account().contains("ACC_GPB_001"));
        assertTrue(group1.account().contains("ACC_GPB_002"));

        DispatcherDatasourceDto missingGroup = response.data().datasource().stream()
                .filter(d -> d.jdbcUrl() == null || d.jdbcUrl().isBlank())
                .findFirst()
                .orElse(null);
        assertNotNull(missingGroup);
        assertTrue(missingGroup.account().contains("ACC_UNMAPPED_999"));
    }

    @Test
    @DisplayName("Сквозной тест ProcessingService: опрос живого app-dispatcher + роутинг + процедура + ошибки по ТЗ")
    void testEndToEndProcessingWithLiveDispatcher() throws Exception {
        AppConfig appConfig = createLiveAppConfig();
        ObjectMapper mapper = new ObjectMapper();
        DispatcherClient liveDispatcherClient = new DispatcherClient(appConfig, mapper, HttpClient.newHttpClient());

        ProcessingService service = new ProcessingService();

        // Моки и сервисы
        TransDao transDao = Mockito.mock(TransDao.class);
        AppControllerClient appControllerClient = Mockito.mock(AppControllerClient.class);
        Mockito.when(appControllerClient.checkAndRouteAccounts(Mockito.any())).thenReturn(true);

        ProcedureDao procedureDao = new ProcedureDao();
        DynamicDataSourceService dynamicDataSourceService = new DynamicDataSourceService();
        injectField(dynamicDataSourceService, "defaultJdbcUrl", java.util.Optional.of("jdbc:postgresql://localhost:5432/changer_db"));
        injectField(dynamicDataSourceService, "defaultUsername", java.util.Optional.of("changer_user"));
        injectField(dynamicDataSourceService, "defaultPassword", java.util.Optional.of("root"));

        // Внедряем зависимости через reflection
        injectField(service, "transDao", transDao);
        injectField(service, "appControllerClient", appControllerClient);
        injectField(service, "dispatcherClient", liveDispatcherClient);
        injectField(service, "procedureDao", procedureDao);
        injectField(service, "dynamicDataSourceService", dynamicDataSourceService);

        String requestId = UUID.randomUUID().toString();
        AppChangerTrans trans = new AppChangerTrans();
        trans.id = 100L;
        trans.requestId = requestId;
        // 3 события:
        // 1) ACC_GPB_001 -> есть в базе диспетчера, jdbc:postgresql://localhost:5432/changer_db (процедура выполнится -> SUCCESS)
        // 2) ACC_GPB_003 -> есть в базе диспетчера, но база :5433 недоступна -> DB_UNREACHABLE
        // 3) ACC_NO_URL -> нет в базе диспетчера -> url пустой -> EMPTY_JDBC_URL
        trans.reqData = String.format("""
                {
                  "requestId": "%s",
                  "eventType": "BALANCE",
                  "entityType": "ACCOUNT",
                  "events": [
                    { "id": "1", "systemAccount": "ACC_GPB_001", "operation": "CR", "xalfa": 500.0, "currency": "RUB" },
                    { "id": "2", "systemAccount": "ACC_GPB_003", "operation": "DR", "xalfa": 100.0, "currency": "RUB" },
                    { "id": "3", "systemAccount": "ACC_NO_URL",  "operation": "CR", "xalfa": 50.0,  "currency": "RUB" }
                  ]
                }
                """, requestId);

        service.processTransaction(trans);

        assertEquals("READY_TO_SEND", trans.status);
        assertEquals("0", trans.respCode);
        assertEquals("PARTIAL_SUCCESS", trans.respDesc);

        ResponseKafkaDto resultDto = JsonUtils.fromJson(trans.respData, ResponseKafkaDto.class);
        assertNotNull(resultDto);
        assertEquals(3, resultDto.events().size());

        // Проверяем событие 1: ACC_GPB_001 успешно посчитан реальной процедурой
        ResponseEventDto event1 = resultDto.events().stream().filter(e -> "1".equals(e.entityId())).findFirst().orElseThrow();
        assertEquals("SUCCESS", event1.status());
        assertNotNull(event1.data());
        assertNotNull(event1.data().newTbal());
        assertNull(event1.error());
        System.out.println("Событие 1 (ACC_GPB_001): SUCCESS, newTbal=" + event1.data().newTbal());

        // Проверяем событие 2: ACC_GPB_003 недоступная база -> DB_UNREACHABLE
        ResponseEventDto event2 = resultDto.events().stream().filter(e -> "2".equals(e.entityId())).findFirst().orElseThrow();
        assertEquals("ERROR", event2.status());
        assertEquals("DB_UNREACHABLE", event2.error().code());
        System.out.println("Событие 2 (ACC_GPB_003): ERROR, code=" + event2.error().code());

        // Проверяем событие 3: ACC_NO_URL пустой url -> EMPTY_JDBC_URL
        ResponseEventDto event3 = resultDto.events().stream().filter(e -> "3".equals(e.entityId())).findFirst().orElseThrow();
        assertEquals("ERROR", event3.status());
        assertEquals("EMPTY_JDBC_URL", event3.error().code());
        System.out.println("Событие 3 (ACC_NO_URL): ERROR, code=" + event3.error().code());
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
