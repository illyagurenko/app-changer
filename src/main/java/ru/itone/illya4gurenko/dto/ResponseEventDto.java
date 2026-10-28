package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * элемент массива events для ответа
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ResponseEventDto(
        String entityValue,
        String entityId,
        String status,
        AccountBalanceDto data,
        ErrorDto error
) {}