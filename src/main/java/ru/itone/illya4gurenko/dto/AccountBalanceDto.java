package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.math.BigDecimal;

/**
 * блок данных с балансами при успешной обработке счета
 */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AccountBalanceDto(
        BigDecimal oldTbal,
        BigDecimal newTbal,
        String operation
) {}
