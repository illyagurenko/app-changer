package ru.itone.illya4gurenko.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.ZonedDateTime;

/**
 * сущность таблицы аудита сообщений
 */
@Entity
@Table(name = "APP_CHANGER_IO_MSGS", schema = "ACC_APP_CHANGER")
public class AppChangerIoMsg extends PanacheEntityBase {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    public Long id;

    @Column(name = "TRANS_ID")
    public Long transId;

    @Column(name = "PROTOCOL_TYPE")
    public String protocolType;

    @Column(name = "MSG_TYPE")
    public String msgType;

    @Column(name = "DIR")
    public String dir;

    @Column(name = "MSG")
    public String msg;

    @Column(name = "NODE_ID")
    public String nodeId;

    @Column(name = "INS_TS")
    public ZonedDateTime insTs;
}