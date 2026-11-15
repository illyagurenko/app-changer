package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.math.BigDecimal;

/**
 * элемент массива events из входящего запроса
 */
@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
public record RequestEventDto(
        String id,
        String systemAccount,
        String currency,
        @JsonAlias({"XALFA", "xalfa"})
        BigDecimal xalfa,
        String operation
) {}
