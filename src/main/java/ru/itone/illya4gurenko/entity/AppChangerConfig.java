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
 * сущность таблицы настроек приложения
 */
@Entity
@Table(name = "APP_CHANGER_CONFIG", schema = "ACC_APP_CHANGER")
public class AppChangerConfig extends PanacheEntityBase {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    public Long id;

    @Column(name = "SYSTEMID")
    public String systemId;

    @Column(name = "EVENT_TYPE")
    public String eventType;

    @Column(name = "ENTITY_TYPE")
    public String entityType;

    @Column(name = "PROTOCOL_TYPE")
    public String protocolType;

    @Column(name = "PROTOCOL_IN")
    public String protocolIn;

    @Column(name = "STATUS_IN")
    public Integer statusIn;

    @Column(name = "PROTOCOL_OUT")
    public String protocolOut;

    @Column(name = "STATUS_OUT")
    public Integer statusOut;

    @Column(name = "INS_TS")
    public ZonedDateTime insTs;

    @Column(name = "UPD_TS")
    public ZonedDateTime updTs;
}