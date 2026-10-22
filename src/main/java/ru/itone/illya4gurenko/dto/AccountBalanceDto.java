package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.math.BigDecimal;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AccountBalanceDto(
        BigDecimal oldTbal,
        BigDecimal newTbal,
        String operation
) {}