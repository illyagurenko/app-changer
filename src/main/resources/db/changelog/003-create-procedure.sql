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

    v_accounts  VARCHAR[] := '{}';
    v_old_tbals NUMERIC(23, 3)[] := '{}';
    v_new_tbals NUMERIC(23, 3)[] := '{}';
BEGIN
    code := 0;
    message := NULL;

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

        v_accounts  := array_append(v_accounts, v_account);
        v_old_tbals := array_append(v_old_tbals, v_old_tbal);
        v_new_tbals := array_append(v_new_tbals, v_new_tbal);
    END LOOP;

    OPEN cur_out FOR
        SELECT a AS "account", o AS "oldTbal", n AS "newTbal"
        FROM unnest(v_accounts, v_old_tbals, v_new_tbals) AS t(a, o, n);

EXCEPTION WHEN OTHERS THEN
    code := 1;
    message := SQLERRM;
END;
$$;