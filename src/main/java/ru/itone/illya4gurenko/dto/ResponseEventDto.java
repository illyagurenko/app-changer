package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ResponseEventDto(
        String entityValue,
        Long entityId,
        String status,
        AccountBalanceDto data,
        ErrorDto error
) {}