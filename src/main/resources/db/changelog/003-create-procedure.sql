--liquibase formatted sql

--changeset illya:006-create-changer-procedure splitStatements:false runOnChange:true
CREATE OR REPLACE PROCEDURE "ACC_APP_CHANGER".changer_data_account(
    IN cur_in_name VARCHAR,
    INOUT cur_out_name VARCHAR,
    INOUT code INT,
    INOUT message VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    cur_in refcursor := cur_in_name;
    cur_out refcursor := cur_out_name;

    v_account   VARCHAR;
    v_operation VARCHAR;
    v_amount    NUMERIC(23, 3);
    v_currency  VARCHAR;

    v_old_tbal  NUMERIC(23, 3);
    v_new_tbal  NUMERIC(23, 3);
BEGIN
    code := 0;
    message := NULL;

    CREATE TEMP TABLE IF NOT EXISTS temp_account_balances (
        "account" VARCHAR,
        "oldTbal" NUMERIC(23, 3),
        "newTbal" NUMERIC(23, 3)
    ) ON COMMIT DROP;

    DELETE FROM temp_account_balances;

    LOOP
        FETCH cur_in INTO v_account, v_operation, v_amount, v_currency;
        EXIT WHEN NOT FOUND;

        v_old_tbal := ROUND((random() * 49000 + 1000)::numeric, 2);

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

    OPEN cur_out FOR SELECT "account", "oldTbal", "newTbal" FROM temp_account_balances;

EXCEPTION WHEN OTHERS THEN
    code := 1;
    message := SQLERRM;
END;
$$;