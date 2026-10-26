package ru.itone.illya4gurenko.dto;

public record ReadyToSendRecord(
        long id,
        String requestId,
        String systemId,
        String protocolType,
        String respData
) {}
