package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.LockModeType;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * работа с транзакционной таблицей
 */
@ApplicationScoped
public class TransDao {

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

    public List<Long> findIdsByStatus(String status, int limit) {
        return AppChangerTrans.find("status", status)
                .project(Long.class)
                .page(0, limit)
                .list();
    }

    public AppChangerTrans findByIdLocked(Long id) {
        return AppChangerTrans.findById(id, LockModeType.PESSIMISTIC_WRITE);
    }
}