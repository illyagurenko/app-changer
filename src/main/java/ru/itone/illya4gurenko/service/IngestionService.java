package ru.itone.illya4gurenko.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.config.AppConfig;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.db.DataSourceProvider;
import ru.itone.illya4gurenko.dto.AppChangerConfigDto;
import ru.itone.illya4gurenko.dto.RequestKafkaDto;
import ru.itone.illya4gurenko.util.JsonUtils;

import java.sql.Connection;
import java.util.UUID;

public class IngestionService {
    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final TransDao transDao;
    private final IoMsgsDao ioMsgsDao;
    private final String nodeId;

    public IngestionService() {
        this.transDao = new TransDao();
        this.ioMsgsDao = new IoMsgsDao();
        this.nodeId = AppConfig.getInstance().getNodeId();
    }

    /**
     * Парсит метаданные и атомарно сохраняет входящее сообщение в TRANS и IO_MSGS.
     */
    public long processIncomingPayload(AppChangerConfigDto config, String rawJson, String recordKey) {
        String requestId = recordKey;
        String systemId = config.systemId();
        String eventType = config.eventType();

        // 1. Извлекаем метаданные из JSON
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
            log.warn("Could not parse JSON metadata, fallback to defaults: {}", e.getMessage());
            if (requestId == null || requestId.isBlank()) {
                requestId = "unknown-req-" + UUID.randomUUID();
            }
        }

        // 2. Атомарное сохранение в БД (PostgreSQL)
        try (Connection conn = DataSourceProvider.getConnection()) {
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
                log.info("Persisted transaction: transId={}, requestId={}, status=RECEIVED", transId, requestId);
                return transId;

            } catch (Exception e) {
                conn.rollback();
                log.error("Failed to persist transaction, rolled back. RequestId: {}", requestId, e);
                throw new RuntimeException("Database error during payload persistence", e);
            }
        } catch (Exception e) {
            log.error("Connection pool error in IngestionService", e);
            throw new RuntimeException(e);
        }
    }
}