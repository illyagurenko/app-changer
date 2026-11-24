package ru.itone.illya4gurenko.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.client.AppControllerClient;
import ru.itone.illya4gurenko.client.DispatcherClient;
import ru.itone.illya4gurenko.dao.ProcedureDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.*;
import ru.itone.illya4gurenko.dto.dispatcher.request.DispatcherRequestBodyDto;
import ru.itone.illya4gurenko.dto.dispatcher.request.DispatcherRequestDataDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherDatasourceDto;
import ru.itone.illya4gurenko.dto.dispatcher.response.DispatcherResponseBodyDto;
import ru.itone.illya4gurenko.entity.AppChangerTrans;
import ru.itone.illya4gurenko.util.JsonUtils;

import java.sql.Connection;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * сервис бизнес-логики обработки транзакций
 */
@ApplicationScoped
public class ProcessingService {
    private static final Logger log = LoggerFactory.getLogger(ProcessingService.class);

    @Inject
    TransDao transDao;

    @Inject
    ProcedureDao procedureDao;

    @Inject
    AppControllerClient appControllerClient;

    @Inject
    DispatcherClient dispatcherClient;

    @Inject
    DynamicDataSourceService dynamicDataSourceService;

    /**
     * захват и обработка одной записи в рамках единой транзакции базы данных
     */
    @Transactional
    public boolean processNext() {
        AppChangerTrans trans = transDao.acquireNextForProcessing();
        if (trans == null) {
            return false;
        }

        executeBusinessLogic(trans);
        return true;
    }

    /**
     * выполнение бизнес-логики: опрос диспетчера, роутинг по базам, запуск процедуры и расчет балансов
     */
    private void executeBusinessLogic(AppChangerTrans trans) {
        log.debug("processing transaction: id={}, reqid={}", trans.id, trans.requestId);

        try {
            if (trans.reqData == null || trans.reqData.isBlank()) {
                markFailed(trans, "empty req_data");
                return;
            }

            RequestKafkaDto requestDto = JsonUtils.fromJson(trans.reqData, RequestKafkaDto.class);

            // первичная валидация корректности счетов контроллером
            if (!appControllerClient.checkAndRouteAccounts(requestDto.events())) {
                failTransaction(trans, requestDto, "app_controller routing rejected accounts");
                return;
            }

            // получение маппинга счет -> jdbc_url от app-dispatcher
            Map<String, String> routing;
            try {
                routing = resolveAccountRouting(trans.requestId, requestDto.events());
            } catch (Exception e) {
                log.error("failed to query app-dispatcher for requestId={}", trans.requestId, e);
                failTransaction(trans, requestDto, "app_dispatcher call failed: " + e.getMessage());
                return;
            }

            // обработка событий: проверка пустых url, доступности бд и вызов процедуры
            List<ResponseEventDto> responseEvents = processAllEvents(requestDto.events(), routing);

            // формирование и сохранение ответа для отправки в кафку
            saveReadyResponse(trans, requestDto, responseEvents);

        } catch (Exception e) {
            log.error("error during business processing of id={}", trans.id, e);
            markFailed(trans, e.getMessage());
        }
    }

    /**
     * запрос источников данных в app-dispatcher и построение карты счет -> jdbc_url
     */
    private Map<String, String> resolveAccountRouting(String requestId, List<RequestEventDto> events) throws Exception {
        List<String> distinctAccounts = events.stream()
                .map(RequestEventDto::systemAccount)
                .filter(acc -> acc != null && !acc.isBlank())
                .distinct()
                .toList();

        DispatcherRequestBodyDto request = new DispatcherRequestBodyDto(
                new DispatcherRequestDataDto("APP_CHANGER", requestId, distinctAccounts),
                "Success",
                null
        );

        DispatcherResponseBodyDto response = dispatcherClient.sendPost(request);

        Map<String, String> routingMap = new HashMap<>();
        if (response != null && response.data() != null && response.data().datasource() != null) {
            for (DispatcherDatasourceDto ds : response.data().datasource()) {
                String url = ds.jdbcUrl();
                if (ds.account() != null) {
                    for (String acc : ds.account()) {
                        if (acc != null) {
                            routingMap.put(acc.trim(), url != null ? url.trim() : null);
                        }
                    }
                }
            }
        }
        return routingMap;
    }

    /**
     * обработка всех событий пачки с группировкой по базам и сохранением исходного порядка
     */
    private List<ResponseEventDto> processAllEvents(List<RequestEventDto> events, Map<String, String> routing) {
        Map<String, ResponseEventDto> eventResultsById = new HashMap<>();
        Map<String, List<RequestEventDto>> eventsByValidUrl = new HashMap<>();

        // разделение событий: пустые url бракуем сразу, валидные группируем по бд
        for (RequestEventDto event : events) {
            String account = event.systemAccount() != null ? event.systemAccount().trim() : null;
            String jdbcUrl = account != null ? routing.get(account) : null;

            if (jdbcUrl == null || jdbcUrl.isBlank() || "null".equalsIgnoreCase(jdbcUrl)) {
                log.warn("Account {} has empty or null jdbc_url", account);
                eventResultsById.put(event.id(), new ResponseEventDto(
                        event.systemAccount(),
                        event.id(),
                        "ERROR",
                        null,
                        new ErrorDto("EMPTY_JDBC_URL", "JDBC_URL is empty for account: " + account)
                ));
            } else {
                eventsByValidUrl.computeIfAbsent(jdbcUrl, k -> new ArrayList<>()).add(event);
            }
        }

        // выполнение процедуры для каждой группы баз данных
        for (Map.Entry<String, List<RequestEventDto>> entry : eventsByValidUrl.entrySet()) {
            executeOnDb(entry.getKey(), entry.getValue(), eventResultsById);
        }

        // сборка итогового списка с сохранением исходного порядка
        List<ResponseEventDto> orderedResults = new ArrayList<>(events.size());
        for (RequestEventDto orig : events) {
            ResponseEventDto res = eventResultsById.get(orig.id());
            if (res != null) {
                orderedResults.add(res);
            }
        }
        return orderedResults;
    }

    /**
     * подключение к бд и вызов хранимой процедуры для группы счетов
     */
    private void executeOnDb(String jdbcUrl, List<RequestEventDto> eventsForDb, Map<String, ResponseEventDto> results) {
        try (Connection conn = dynamicDataSourceService.getConnection(jdbcUrl)) {
            ProcedureResult procResult = procedureDao.executeOnConnection(conn, eventsForDb);

            if (procResult.code() == 0) {
                for (RequestEventDto ev : eventsForDb) {
                    ProcedureResult.CalculatedBalance bal = procResult.balancesByAccount().get(ev.systemAccount());
                    AccountBalanceDto balanceDto = bal != null
                            ? new AccountBalanceDto(bal.oldTbal(), bal.newTbal(), ev.operation())
                            : null;
                    results.put(ev.id(), new ResponseEventDto(
                            ev.systemAccount(),
                            ev.id(),
                            "SUCCESS",
                            balanceDto,
                            null
                    ));
                }
            } else {
                log.error("procedure failed on db {}: code={}, msg={}", jdbcUrl, procResult.code(), procResult.message());
                for (RequestEventDto ev : eventsForDb) {
                    results.put(ev.id(), new ResponseEventDto(
                            ev.systemAccount(),
                            ev.id(),
                            "ERROR",
                            null,
                            new ErrorDto(String.valueOf(procResult.code()), procResult.message())
                    ));
                }
            }
        } catch (Exception e) {
            // ошибка подключения или несуществующая база данных по ТЗ
            log.error("failed to connect or execute on db {}: {}", jdbcUrl, e.getMessage());
            for (RequestEventDto ev : eventsForDb) {
                results.put(ev.id(), new ResponseEventDto(
                        ev.systemAccount(),
                        ev.id(),
                        "ERROR",
                        null,
                        new ErrorDto("DB_UNREACHABLE", "Cannot connect to database: " + jdbcUrl)
                ));
            }
        }
    }

    /**
     * сохранение готового ответа в транзакционную таблицу и перевод в статус ready_to_send
     */
    private void saveReadyResponse(AppChangerTrans trans, RequestKafkaDto requestDto, List<ResponseEventDto> responseEvents) {
        boolean anySuccess = responseEvents.stream().anyMatch(e -> "SUCCESS".equalsIgnoreCase(e.status()));
        boolean allSuccess = responseEvents.stream().allMatch(e -> "SUCCESS".equalsIgnoreCase(e.status()));

        ResponseKafkaDto responseDto = new ResponseKafkaDto(
                System.currentTimeMillis(),
                "SMART",
                trans.requestId,
                requestDto.eventType() != null ? requestDto.eventType() : "BALANCE",
                requestDto.entityType() != null ? requestDto.entityType() : "ACCOUNT",
                "SUCCESS",
                null,
                responseEvents
        );

        trans.respData = JsonUtils.toJson(responseDto);
        trans.status = "READY_TO_SEND";
        trans.respCode = "0";
        trans.respDesc = allSuccess ? "SUCCESS" : (anySuccess ? "PARTIAL_SUCCESS" : "ALL_EVENTS_ERROR");
        trans.updTs = ZonedDateTime.now();

        log.debug("transaction id={} successfully processed -> ready_to_send (desc={})", trans.id, trans.respDesc);
    }

    /**
     * формирование ответа с ошибкой маршрутизации и перевод в статус ready_to_send
     */
    private void failTransaction(AppChangerTrans trans, RequestKafkaDto requestDto, String errorMsg) {
        String safeErrorMsg = truncate(errorMsg, 255);
        ResponseKafkaDto errorResponse = new ResponseKafkaDto(
                System.currentTimeMillis(),
                "SMART",
                trans.requestId,
                requestDto.eventType(),
                requestDto.entityType(),
                "ERROR",
                new ErrorDto("ROUTING_ERR", safeErrorMsg),
                null
        );
        trans.respData = JsonUtils.toJson(errorResponse);
        trans.status = "READY_TO_SEND";
        trans.respCode = "ROUTING_ERR";
        trans.respDesc = safeErrorMsg;
        trans.updTs = ZonedDateTime.now();
    }

    /**
     * перевод транзакции в статус ошибки обработки processing_error
     */
    private void markFailed(AppChangerTrans trans, String errorMsg) {
        trans.status = "PROCESSING_ERROR";
        trans.respDesc = truncate(errorMsg, 255);
        trans.updTs = ZonedDateTime.now();
    }

    /**
     * обрезка строки по максимальной длине
     */
    private static String truncate(String str, int maxLen) {
        if (str == null) return null;
        return str.length() > maxLen ? str.substring(0, maxLen) : str;
    }

    /**
     * обработка конкретной транзакции по сущности
     */
    @Transactional
    public void processTransaction(AppChangerTrans trans) {
        if (trans == null) {
            return;
        }
        executeBusinessLogic(trans);
    }

    /**
     * обработка транзакции по идентификатору для тестов
     */
    @Transactional
    public void processTransaction(Long transId) {
        AppChangerTrans trans = transDao.findById(transId);
        if (trans == null) {
            return;
        }
        executeBusinessLogic(trans);
    }
}
