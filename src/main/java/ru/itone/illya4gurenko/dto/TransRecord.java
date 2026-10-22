package ru.itone.illya4gurenko.dto;

public record TransRecord(
        long id,
        String requestId,
        String systemId,
        String eventType,
        String protocolType,
        String reqData
) {}