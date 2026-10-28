package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * блок с описанием ошибки
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorDto(
        String code,
        String message
) {}