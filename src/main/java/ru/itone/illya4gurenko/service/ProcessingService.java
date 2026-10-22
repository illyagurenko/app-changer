package ru.itone.illya4gurenko.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.ProcedureDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.db.DataSourceProvider;
import ru.itone.illya4gurenko.dto.*;
import ru.itone.illya4gurenko.util.JsonUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class ProcessingService {
    private static final Logger log = LoggerFactory.getLogger(ProcessingService.class);

    private final TransDao transDao;
    private final ProcedureDao procedureDao;
    private final AppControllerClient appControllerClient;

    public ProcessingService() {
        this.transDao = new TransDao();
        this.procedureDao = new ProcedureDao();
        this.appControllerClient = new AppControllerClient();
    }

    public void processTransaction(TransRecord record) {
        long transId = record.id();
        String requestId = record.requestId();
        log.info("Processing transaction: id={}, requestId={}", transId, requestId);

        try (Connection conn = DataSourceProvider.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // 1. Меняем статус на IN_PROCESS
                transDao.updateStatus(conn, transId, "IN_PROCESS");
                conn.commit(); // фиксируем переход в обработку
            } catch (Exception e) {
                conn.rollback();
                log.error("Failed to update status to IN_PROCESS for transId={}", transId, e);
                return;
            }

            // Начинаем бизнес-транзакцию
            try {
                // 2. Разбираем вычитанный JSON
                RequestKafkaDto requestDto = JsonUtils.fromJson(record.reqData(), RequestKafkaDto.class);

                // 3. Отправляем счета в APP_CONTROLLER
                boolean routingOk = appControllerClient.checkAndRouteAccounts(requestDto.events());
                if (!routingOk) {
                    failTransaction(conn, transId, requestId, requestDto, "APP_CONTROLLER routing rejected accounts");
                    return;
                }

                // 4. Вызываем процедуру changer_data_account
                ProcedureResult procResult = procedureDao.executeChangerDataAccount(conn, requestDto.events());

                // 5. Формируем ответ ResponseKafkaDto
                ResponseKafkaDto responseDto;
                String respCode;
                String respDesc;

                if (procResult.code() == 0) {
                    // Успех: собираем данные с новыми балансами
                    List<ResponseEventDto> responseEvents = new ArrayList<>();

                    for (RequestEventDto event : requestDto.events()) {
                        ProcedureResult.CalculatedBalance bal = procResult.balancesByAccount().get(event.systemAccount());

                        AccountBalanceDto balanceDto = null;
                        if (bal != null) {
                            balanceDto = new AccountBalanceDto(bal.oldTbal(), bal.newTbal(), event.operation());
                        }

                        responseEvents.add(new ResponseEventDto(
                                event.systemAccount(),
                                event.id(),
                                "SUCCESS",
                                balanceDto,
                                null
                        ));
                    }

                    responseDto = new ResponseKafkaDto(
                            System.currentTimeMillis(),
                            "SMART",
                            requestId,
                            requestDto.eventType() != null ? requestDto.eventType() : "BALANCE",
                            requestDto.entityType() != null ? requestDto.entityType() : "ACCOUNT",
                            "SUCCESS",
                            null,
                            responseEvents
                    );
                    respCode = "0";
                    respDesc = "SUCCESS";

                } else {
                    // Ошибка из процедуры
                    ErrorDto errorDto = new ErrorDto(String.valueOf(procResult.code()), procResult.message());
                    responseDto = new ResponseKafkaDto(
                            System.currentTimeMillis(),
                            "SMART",
                            requestId,
                            requestDto.eventType(),
                            requestDto.entityType(),
                            "ERROR",
                            errorDto,
                            null
                    );
                    respCode = String.valueOf(procResult.code());
                    respDesc = procResult.message();
                }

                // 6. Сериализуем ответ в JSON и пишем в resp_DATA со статусом READY_TO_SEND
                String respJson = JsonUtils.toJson(responseDto);
                transDao.updateResponse(conn, transId, respJson, "READY_TO_SEND", respCode, respDesc);

                conn.commit();
                log.info("Transaction id={} successfully processed -> READY_TO_SEND", transId);

            } catch (Exception e) {
                conn.rollback();
                log.error("Error during business processing of transId={}", transId, e);
                markFailed(transId, e.getMessage());
            }
        } catch (Exception e) {
            log.error("Database connection error in ProcessingService for transId={}", transId, e);
        }
    }

    private void failTransaction(Connection conn, long transId, String requestId, RequestKafkaDto requestDto, String errorMsg) throws Exception {
        ResponseKafkaDto errorResponse = new ResponseKafkaDto(
                System.currentTimeMillis(),
                "SMART",
                requestId,
                requestDto.eventType(),
                requestDto.entityType(),
                "ERROR",
                new ErrorDto("ROUTING_ERR", errorMsg),
                null
        );

        String respJson = JsonUtils.toJson(errorResponse);
        transDao.updateResponse(conn, transId, respJson, "READY_TO_SEND", "ROUTING_ERR", errorMsg);
        conn.commit();
    }

    private void markFailed(long transId, String errorMsg) {
        try (Connection conn = DataSourceProvider.getConnection()) {
            transDao.updateStatus(conn, transId, "PROCESSING_ERROR");
        } catch (Exception ex) {
            log.error("Failed to set PROCESSING_ERROR for transId={}", transId, ex);
        }
    }
}