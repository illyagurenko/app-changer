package ru.itone.illya4gurenko.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.ProcedureDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.*;
import ru.itone.illya4gurenko.entity.AppChangerTrans;
import ru.itone.illya4gurenko.util.JsonUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

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


    private void executeBusinessLogic(AppChangerTrans trans) {
        log.debug("processing transaction: id={}, reqid={}", trans.id, trans.requestId);

        try {
            if (trans.reqData == null || trans.reqData.isBlank()) {
                markFailed(trans, "empty req_data");
                return;
            }

            RequestKafkaDto requestDto = JsonUtils.fromJson(trans.reqData, RequestKafkaDto.class);

            boolean routingOk = appControllerClient.checkAndRouteAccounts(requestDto.events());
            if (!routingOk) {
                failTransaction(trans, requestDto, "app_controller routing rejected accounts");
                return;
            }

            ProcedureResult procResult = procedureDao.executeChangerDataAccount(requestDto.events());
            ResponseKafkaDto responseDto;

            if (procResult.code() == 0) {
                List<ResponseEventDto> responseEvents = new ArrayList<>();
                for (RequestEventDto event : requestDto.events()) {
                    ProcedureResult.CalculatedBalance bal = procResult.balancesByAccount().get(event.systemAccount());
                    AccountBalanceDto balanceDto = bal != null ? new AccountBalanceDto(bal.oldTbal(), bal.newTbal(), event.operation()) : null;

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
                        trans.requestId,
                        requestDto.eventType() != null ? requestDto.eventType() : "BALANCE",
                        requestDto.entityType() != null ? requestDto.entityType() : "ACCOUNT",
                        "SUCCESS",
                        null,
                        responseEvents
                );

                trans.respCode = "0";
                trans.respDesc = "SUCCESS";
            } else {
                ErrorDto errorDto = new ErrorDto(String.valueOf(procResult.code()), procResult.message());
                responseDto = new ResponseKafkaDto(
                        System.currentTimeMillis(),
                        "SMART",
                        trans.requestId,
                        requestDto.eventType(),
                        requestDto.entityType(),
                        "ERROR",
                        errorDto,
                        null
                );
                trans.respCode = String.valueOf(procResult.code());
                trans.respDesc = truncate(procResult.message(), 255);
            }

            trans.respData = JsonUtils.toJson(responseDto);
            trans.status = "READY_TO_SEND";
            trans.updTs = ZonedDateTime.now();

            log.debug("transaction id={} successfully processed -> ready_to_send", trans.id);

        } catch (Exception e) {
            log.error("error during business processing of id={}", trans.id, e);
            markFailed(trans, e.getMessage());
        }
    }

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

    private void markFailed(AppChangerTrans trans, String errorMsg) {
        trans.status = "PROCESSING_ERROR";
        trans.respDesc = truncate(errorMsg, 255);
        trans.updTs = ZonedDateTime.now();
    }

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