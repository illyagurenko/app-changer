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
 * сервис отправки готовых ответов в кафку
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
     * захват и отправка одного сообщения в рамках единой транзакции базы данных
     */
    @Transactional
    public boolean dispatchNext(KafkaProducer<String, String> producer, String topicOut) {
        AppChangerTrans trans = transDao.acquireNextForSending();
        if (trans == null) {
            return false;
        }

        sendAndFinalize(trans, producer, topicOut);
        return true;
    }

    /**
     * отправка конкретной транзакции
     */
    @Transactional
    public void dispatchTransaction(AppChangerTrans trans, KafkaProducer<String, String> producer, String topicOut) {
        if (trans == null) {
            return;
        }
        sendAndFinalize(trans, producer, topicOut);
    }

    private void sendAndFinalize(AppChangerTrans trans, KafkaProducer<String, String> producer, String topicOut) {
        log.debug("dispatching response: id={}, reqid={}, topic={}", trans.id, trans.requestId, topicOut);

        try {
            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topicOut, trans.requestId, trans.respData);
            producer.send(kafkaRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("failed to deliver message to kafka: id={}", trans.id, exception);
                }
            });

            ioMsgsDao.insertIoMsg(trans.id, trans.protocolType, trans.systemId, "OUT", trans.respData, nodeId);

            trans.status = "COMPLETED";
            trans.updTs = ZonedDateTime.now();

            log.debug("transaction id={} finalized with status=completed", trans.id);
        } catch (Exception e) {
            log.error("failed to send response to kafka for reqid={}", trans.requestId, e);
        }
    }
}