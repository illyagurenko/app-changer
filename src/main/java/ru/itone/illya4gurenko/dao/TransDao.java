package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.LockModeType;
import jakarta.transaction.Transactional;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * работа с транзакционной таблицей
 */
@ApplicationScoped
public class TransDao {

    /**
     * сохраняет входящую транзакцию
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
     * выбирает id транзакций в нужном статусе в рамках отдельной транзакции
     */
    @Transactional
    public List<Long> findIdsByStatus(String status, int limit) {
        return AppChangerTrans.getEntityManager()
                .createQuery("SELECT t.id FROM AppChangerTrans t WHERE t.status = :status ORDER BY t.id ASC", Long.class)
                .setParameter("status", status)
                .setMaxResults(limit)
                .getResultList();
    }

    /**
     * блокирует и возвращает транзакцию по id
     */
    public AppChangerTrans findByIdLocked(Long id) {
        return AppChangerTrans.findById(id, LockModeType.PESSIMISTIC_WRITE);
    }
}