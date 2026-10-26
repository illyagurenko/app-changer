package ru.itone.illya4gurenko.service;


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dao.TransDao;
import ru.itone.illya4gurenko.dto.ReadyToSendRecord;

import javax.sql.DataSource;
import java.sql.Connection;

@ApplicationScoped
public class ProduceService {
    private static final Logger log = LoggerFactory.getLogger(ProduceService.class);

    @Inject
    DataSource dataSource;

    @Inject
    TransDao transDao;

    @Inject
    IoMsgsDao ioMsgsDao;

    @ConfigProperty(name = "app.node-id", defaultValue = "162")
    String nodeId;

    public void dispatch(KafkaProducer<String, String> producer, String topicOut, ReadyToSendRecord record) {
        long transId = record.id();
        String requestId = record.requestId();
        String respJson = record.respData();

        log.info("dispatching response: transId={}, requestId={}, topic={}", transId, requestId, topicOut);

        try {
            // отправляем в Kafka синхронно ждем подтверждения брокера
            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topicOut, requestId, respJson);
            producer.send(kafkaRecord).get();
            log.info("response successfully sent to Kafka. requestId={}", requestId);

            // атомарно пишем аудит OUT и завершаем транзакцию в БД
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    ioMsgsDao.insertIoMsg(
                            conn,
                            transId,
                            record.protocolType(),
                            record.systemId(),
                            "OUT",
                            respJson,
                            nodeId
                    );

                    transDao.updateCompleted(conn, transId);
                    conn.commit();

                    log.info("transaction id={} finalized with status=COMPLETED", transId);

                } catch (Exception e) {
                    conn.rollback();
                    log.error("failed to update status to COMPLETED for transId={}", transId, e);
                }
            }
        } catch (Exception e) {
            log.error("failed to send response to Kafka for requestId={}", requestId, e);
        }
    }
}