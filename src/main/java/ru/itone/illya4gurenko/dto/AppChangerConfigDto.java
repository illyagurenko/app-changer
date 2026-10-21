package ru.itone.illya4gurenko.dto;

public record AppChangerConfigDto(
        Long id,
        String systemId,
        String eventType,
        String entityType,
        String protocolType,
        String protocolIn,
        int statusIn,
        String protocolOut,
        int statusOut
) {}
