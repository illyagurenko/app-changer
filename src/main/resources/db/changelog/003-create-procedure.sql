--liquibase formatted sql

--changeset illya:006-create-changer-procedure splitStatements:false
CREATE OR REPLACE PROCEDURE "ACC_APP_CHANGER".changer_data_account(
    IN cur_in refcursor, -- передаю я
    INOUT cur_out refcursor, -- передаст процедура
    OUT code INT,
    OUT message VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_account   VARCHAR;
    v_operation VARCHAR;
    v_amount    NUMERIC(23, 3);
    v_currency  VARCHAR;

    v_old_tbal  NUMERIC(23, 3);
    v_new_tbal  NUMERIC(23, 3);
BEGIN
    code := 0;
    message := NULL;

    -- временная таблица для чтения и изменения
    CREATE TEMP TABLE IF NOT EXISTS temp_account_balances (
        "account" VARCHAR,
        "oldTbal" NUMERIC(23, 3),
        "newTbal" NUMERIC(23, 3)
    ) ON COMMIT DROP;

    DELETE FROM temp_account_balances;

    LOOP
        -- чтение из входного курсора
        FETCH cur_in INTO v_account, v_operation, v_amount, v_currency;
        EXIT WHEN NOT FOUND;

        --  генерация баланса до изменений (например, от 1000 до 50000)
        v_old_tbal := ROUND((random() * 49000 + 1000)::numeric, 2);

        --  newTbal в зависимости от DR  CR  ZR
        CASE UPPER(TRIM(v_operation))
            WHEN 'CR' THEN
                v_new_tbal := v_old_tbal + v_amount;
            WHEN 'DR' THEN
                v_new_tbal := v_old_tbal - v_amount;
            WHEN 'ZR' THEN
                v_new_tbal := 0.00;
            ELSE
                v_new_tbal := v_old_tbal;
        END CASE;

        INSERT INTO temp_account_balances ("account", "oldTbal", "newTbal")
        VALUES (v_account, v_old_tbal, v_new_tbal);
    END LOOP;

    -- открываем выходной курсор
    OPEN cur_out FOR SELECT "account", "oldTbal", "newTbal" FROM temp_account_balances;

EXCEPTION WHEN OTHERS THEN
    code := 1;
    message := SQLERRM;
END;
$$;