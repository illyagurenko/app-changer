--liquibase formatted sql

--changeset illya:005-insert-default-config
INSERT INTO "ACC_APP_CHANGER"."APP_CHANGER_CONFIG" (
    "SYSTEMID",
    "EVENT_TYPE",
    "ENTITY_TYPE",
    "PROTOCOL_TYPE",
    "PROTOCOL_IN",
    "STATUS_IN",
    "PROTOCOL_OUT",
    "STATUS_OUT",
    "INS_TS",
    "UPD_TS"
) VALUES (
    'GRU',
    'BALANCE',
    'ACCOUNT',
    'KAFKA',
    'gru.balance.request',
    1,
    'gru.balance.response',
    1,
    now(),
    now()
) ON CONFLICT ("SYSTEMID") DO NOTHING;