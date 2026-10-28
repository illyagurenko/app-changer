package ru.itone.illya4gurenko.dto;

import java.math.BigDecimal;
import java.util.Map;

/**
 * результат работы хранимой процедуры базы данных
 */
public record ProcedureResult(
        int code,
        String message,
        Map<String, CalculatedBalance> balancesByAccount
) {
    /**
     * рассчитанные балансы до и после операции
     */
    public record CalculatedBalance(BigDecimal oldTbal, BigDecimal newTbal) {}
}