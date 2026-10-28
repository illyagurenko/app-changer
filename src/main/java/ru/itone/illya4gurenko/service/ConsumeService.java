package ru.itone.illya4gurenko.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.entity.AppChangerConfig;
import ru.itone.illya4gurenko.entity.AppChangerTrans;
import ru.itone.illya4gurenko.dto.RequestKafkaDto;
import ru.itone.illya4gurenko.util.JsonUtils;

import java.util.UUID;

/**
 * сервис сохранения входящих сообщений в бд
 */
@ApplicationScoped
public class ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeService.class);

    @Inject
    TransDao transDao;

    @Inject
    IoMsgsDao ioMsgsDao;

    @ConfigProperty(name = "app.node-id", defaultValue = "162")
    String nodeId;

    /**
     * разбирает сырой json и сохраняет транзакцию и аудит
     */
    @Transactional
    public void readAndSaveDb(AppChangerConfig config, String rawJson, String recordKey) {
        if (rawJson == null || rawJson.isBlank()) {
            log.warn("received empty message from kafka, skipping");
            return;
        }

        String requestId = recordKey;
        String systemId = config.systemId;
        String eventType = config.eventType;

        try {
            RequestKafkaDto dto = JsonUtils.fromJson(rawJson, RequestKafkaDto.class);
            if (dto.requestId() != null && !dto.requestId().isBlank()) requestId = dto.requestId();
            if (dto.systemId() != null && !dto.systemId().isBlank()) systemId = dto.systemId();
            if (dto.eventType() != null && !dto.eventType().isBlank()) eventType = dto.eventType();
        } catch (Exception e) {
            log.warn("could not parse metadata from json: {}", e.getMessage());
            if (requestId == null || requestId.isBlank()) {
                requestId = "unknown-req-" + UUID.randomUUID();
            }
        }

        AppChangerTrans trans = transDao.insertReceived(config.protocolType, systemId, requestId, eventType, rawJson);
        ioMsgsDao.insertIoMsg(trans.id, config.protocolType, systemId, "IN", rawJson, nodeId);

        log.info("persisted transaction: id={}, reqid={}, status=received", trans.id, requestId);
    }
}