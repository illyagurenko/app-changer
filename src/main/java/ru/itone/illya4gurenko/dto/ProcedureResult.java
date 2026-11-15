package ru.itone.illya4gurenko.dto;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.math.BigDecimal;
import java.util.Map;

/**
 * результат работы хранимой процедуры базы данных
 */
@RegisterForReflection
public record ProcedureResult(
        int code,
        String message,
        Map<String, CalculatedBalance> balancesByAccount
) {
    /**
     * рассчитанные балансы до и после операции
     */
    @RegisterForReflection
    public record CalculatedBalance(BigDecimal oldTbal, BigDecimal newTbal) {}
}
