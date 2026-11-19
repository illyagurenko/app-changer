package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import ru.itone.illya4gurenko.dto.IoMsgDto;
import ru.itone.illya4gurenko.entity.AppChangerIoMsg;
import ru.itone.illya4gurenko.entity.AppChangerTrans;

import java.time.ZonedDateTime;

/**
 * запись и чтение аудита сообщений в базе данных
 */
@ApplicationScoped
public class IoMsgsDao {

    /**
     * сохранение входящего или исходящего сообщения
     */
    @Transactional
    public void insertIoMsg(Long transId, String protocolType, String msgType, String dir, String msg, String nodeId) {
        AppChangerIoMsg ioMsg = new AppChangerIoMsg();
        ioMsg.transId = transId;
        ioMsg.protocolType = protocolType;
        ioMsg.msgType = msgType;
        ioMsg.dir = dir;
        ioMsg.msg = msg;
        ioMsg.nodeId = nodeId;
        ioMsg.insTs = ZonedDateTime.now();
        ioMsg.persist();
    }

    /**
     * получение последней записи аудита (опционально с фильтром по направлению IN / OUT)
     */
    public IoMsgDto findLatest(String dir) {
        AppChangerIoMsg ioMsg;
        if (dir != null && !dir.isBlank()) {
            ioMsg = AppChangerIoMsg.find("dir = ?1 order by id desc", dir.trim().toUpperCase()).firstResult();
        } else {
            ioMsg = AppChangerIoMsg.find("order by id desc").firstResult();
        }
        return toDto(ioMsg, null);
    }

    /**
     * получение записи аудита по requestId транзакции и направлению (IN / OUT)
     */
    public IoMsgDto findByRequestIdAndDir(String requestId, String dir) {
        if (requestId == null || requestId.isBlank() || dir == null || dir.isBlank()) {
            return null;
        }
        AppChangerTrans trans = AppChangerTrans.find("requestId = ?1 order by id desc", requestId.trim()).firstResult();
        if (trans == null) {
            return null;
        }
        AppChangerIoMsg ioMsg = AppChangerIoMsg.find(
                "transId = ?1 and dir = ?2 order by id desc",
                trans.id,
                dir.trim().toUpperCase()
        ).firstResult();
        return toDto(ioMsg, trans.requestId);
    }

    private IoMsgDto toDto(AppChangerIoMsg ioMsg, String knownRequestId) {
        if (ioMsg == null) {
            return null;
        }
        String requestId = knownRequestId;
        if (requestId == null && ioMsg.transId != null) {
            AppChangerTrans trans = AppChangerTrans.findById(ioMsg.transId);
            if (trans != null) {
                requestId = trans.requestId;
            }
        }
        return new IoMsgDto(
                ioMsg.id,
                ioMsg.transId,
                requestId,
                ioMsg.protocolType,
                ioMsg.msgType,
                ioMsg.dir,
                ioMsg.nodeId,
                ioMsg.insTs
        );
    }
}
