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
 * сущность главной транзакционной таблицы
 */
@Entity
@Table(name = "APP_CHANGER_TRANS", schema = "ACC_APP_CHANGER")
public class AppChangerTrans extends PanacheEntityBase {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    public Long id;

    @Column(name = "PROTOCOL_TYPE")
    public String protocolType;

    @Column(name = "SYSTEM_ID")
    public String systemId;

    @Column(name = "REQUEST_ID")
    public String requestId;

    @Column(name = "EVENT_TYPE")
    public String eventType;

    @Column(name = "REQ_DATA")
    public String reqData;

    @Column(name = "RESP_DATA")
    public String respData;

    @Column(name = "STATUS")
    public String status;

    @Column(name = "RESP_CODE")
    public String respCode;

    @Column(name = "RESP_DESC")
    public String respDesc;

    @Column(name = "INS_TS")
    public ZonedDateTime insTs;

    @Column(name = "UPD_TS")
    public ZonedDateTime updTs;
}