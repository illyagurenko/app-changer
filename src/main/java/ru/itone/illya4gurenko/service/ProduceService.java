package ru.itone.illya4gurenko.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import java.time.ZonedDateTime;

/**
 * сервис отправки готовых сообщений
 */
@ApplicationScoped
public class ProduceService {
    private static final Logger log = LoggerFactory.getLogger(ProduceService.class);

    @Inject
    TransDao transDao;

    @Inject
    IoMsgsDao ioMsgsDao;

    @ConfigProperty(name = "app.node-id", defaultValue = "162")
    String nodeId;

    /**
     * отправляет данные в кафку и фиксирует изменения в бд
     */
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void dispatchTransaction(Long transId, KafkaProducer<String, String> producer, String topicOut) {
        AppChangerTrans trans = transDao.findByIdLocked(transId);
        if (trans == null || !"READY_TO_SEND".equals(trans.status)) {
            return;
        }

        log.info("dispatching response: id={}, reqid={}, topic={}", trans.id, trans.requestId, topicOut);

        try {
            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topicOut, trans.requestId, trans.respData);
            producer.send(kafkaRecord).get();
            log.info("response sent to kafka. reqid={}", trans.requestId);

            ioMsgsDao.insertIoMsg(trans.id, trans.protocolType, trans.systemId, "OUT", trans.respData, nodeId);

            trans.status = "COMPLETED";
            trans.updTs = ZonedDateTime.now();

            log.info("transaction id={} finalized with status=completed", trans.id);
        } catch (Exception e) {
            log.error("failed to send response to kafka for reqid={}", trans.requestId, e);
        }
    }
}