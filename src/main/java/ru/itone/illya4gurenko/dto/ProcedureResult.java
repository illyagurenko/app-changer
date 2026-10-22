package ru.itone.illya4gurenko.dto;

import java.math.BigDecimal;
import java.util.Map;

/**
 * результат работы хранимой процедуры changer_data_account
 *
 * @param code       код ответа 0 - успех
 * @param message    сообщение об ошибке null если успешно
 * @param balancesByAccount мапа: номер счета -> балансы oldTbal, newTbal
 */
public record ProcedureResult(
        int code,
        String message,
        Map<String, CalculatedBalance> balancesByAccount
) {
    public record CalculatedBalance(BigDecimal oldTbal, BigDecimal newTbal) {}
}