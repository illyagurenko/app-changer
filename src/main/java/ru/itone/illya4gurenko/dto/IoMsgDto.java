package ru.itone.illya4gurenko.dto;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.time.ZonedDateTime;

/**
 * dto записи аудита из таблицы APP_CHANGER_IO_MSGS с привязкой к requestId
 */
@RegisterForReflection
public record IoMsgDto(
        Long id,
        Long transId,
        String requestId,
        String protocolType,
        String msgType,
        String dir,
        String nodeId,
        ZonedDateTime insTs
) {}
