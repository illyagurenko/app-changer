package ru.itone.illya4gurenko.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.AppChangerConfigDto;
import ru.itone.illya4gurenko.dto.RequestKafkaDto;
import ru.itone.illya4gurenko.util.JsonUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.UUID;

@ApplicationScoped
public class ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeService.class);

    @Inject
    DataSource dataSource;

    @Inject
    TransDao transDao;

    @Inject
    IoMsgsDao ioMsgsDao;

    @ConfigProperty(name = "app.node-id", defaultValue = "162")
    String nodeId;

    public long readAndSaveDb(AppChangerConfigDto config, String rawJson, String recordKey) {
        String requestId = recordKey;
        String systemId = config.systemId();
        String eventType = config.eventType();

        // извлекаем метаданные из JSON
        try {
            RequestKafkaDto dto = JsonUtils.fromJson(rawJson, RequestKafkaDto.class);
            if (dto.requestId() != null && !dto.requestId().isBlank()) {
                requestId = dto.requestId();
            }
            if (dto.systemId() != null && !dto.systemId().isBlank()) {
                systemId = dto.systemId();
            }
            if (dto.eventType() != null && !dto.eventType().isBlank()) {
                eventType = dto.eventType();
            }
        } catch (Exception e) {
            log.warn("could not parse JSON metadata: {}", e.getMessage());
            if (requestId == null || requestId.isBlank()) {
                requestId = "unknown-req-" + UUID.randomUUID();
            }
        }

        // сохранение в БД через инжектированный пул Quarkus dataSource
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                long transId = transDao.insertReceived(
                        conn,
                        config.protocolType(),
                        systemId,
                        requestId,
                        eventType,
                        rawJson
                );

                ioMsgsDao.insertIoMsg(
                        conn,
                        transId,
                        config.protocolType(),
                        systemId,
                        "IN",
                        rawJson,
                        nodeId
                );

                conn.commit();
                log.info("persisted transaction: transId={}, requestId={}, status=RECEIVED", transId, requestId);
                return transId;

            } catch (Exception e) {
                conn.rollback();
                log.error("failed to persist transaction, rolled back. RequestId: {}", requestId, e);
                throw new RuntimeException("Database error during payload persistence", e);
            }
        } catch (Exception e) {
            log.error("connection pool error in ConsumeService", e);
            throw new RuntimeException(e);
        }
    }
}