package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * работа с транзакционной таблицей в базе данных
 */
@ApplicationScoped
public class TransDao {

    /**
     * сохранение входящей транзакции в статусе received
     */
    @Transactional
    public AppChangerTrans insertReceived(String protocolType, String systemId, String requestId, String eventType, String reqData) {
        AppChangerTrans trans = new AppChangerTrans();
        trans.protocolType = protocolType;
        trans.systemId = systemId;
        trans.requestId = requestId;
        trans.eventType = eventType;
        trans.reqData = reqData;
        trans.status = "RECEIVED";
        ZonedDateTime now = ZonedDateTime.now();
        trans.insTs = now;
        trans.updTs = now;
        trans.persist();
        return trans;
    }

    /**
     * атомарный захват одной записи для обработки со сменой статуса на in_process
     */
    @Transactional
    public AppChangerTrans acquireNextForProcessing() {
        String sql = """
            SELECT "ID" FROM "ACC_APP_CHANGER"."APP_CHANGER_TRANS"
            WHERE "STATUS" = 'RECEIVED'
            ORDER BY "ID" ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """;

        List<?> results = AppChangerTrans.getEntityManager()
                .createNativeQuery(sql)
                .getResultList();

        if (results.isEmpty()) {
            return null;
        }

        Long id = ((Number) results.getFirst()).longValue();
        AppChangerTrans trans = AppChangerTrans.findById(id);
        if (trans != null) {
            trans.status = "IN_PROCESS";
            trans.updTs = ZonedDateTime.now();
        }
        return trans;
    }

    /**
     * атомарный захват одной записи для отправки со сменой статуса на sending
     */
    @Transactional
    public AppChangerTrans acquireNextForSending() {
        String sql = """
            SELECT "ID" FROM "ACC_APP_CHANGER"."APP_CHANGER_TRANS"
            WHERE "STATUS" = 'READY_TO_SEND'
            ORDER BY "ID" ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """;

        List<?> results = AppChangerTrans.getEntityManager()
                .createNativeQuery(sql)
                .getResultList();

        if (results.isEmpty()) {
            return null;
        }

        Long id = ((Number) results.getFirst()).longValue();
        AppChangerTrans trans = AppChangerTrans.findById(id);
        if (trans != null) {
            trans.status = "SENDING";
            trans.updTs = ZonedDateTime.now();
        }
        return trans;
    }

    /**
     * поиск транзакции по идентификатору
     */
    @Transactional
    public AppChangerTrans findById(Long id) {
        return AppChangerTrans.findById(id);
    }
}