package ru.itone.illya4gurenko.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RequestEventDto(
        Long id,
        String systemAccount,
        String currency,
        @JsonAlias({"XALFA", "xalfa"})
        BigDecimal xalfa,
        String operation
) {}
