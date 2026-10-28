package ru.itone.illya4gurenko.dao;

import jakarta.enterprise.context.ApplicationScoped;
import ru.itone.illya4gurenko.entity.AppChangerIoMsg;

import java.time.ZonedDateTime;

/**
 * сохранение аудита сообщений (ввод/вывод)
 */
@ApplicationScoped
public class IoMsgsDao {

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
}