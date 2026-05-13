DROP PROCEDURE IF EXISTS dm.fill_f101_round_f(DATE);

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id BIGINT;

    v_started_at TIMESTAMP;
    v_error_message TEXT;

    v_from_date DATE;
    v_to_date DATE;
    v_prev_date DATE;

    v_rows_deleted INTEGER := 0;
    v_rows_inserted INTEGER := 0;
BEGIN

    v_started_at := clock_timestamp();

    v_from_date := date_trunc('month', i_OnDate - INTERVAL '1 month')::DATE;
    v_to_date := (date_trunc('month', i_OnDate)::DATE - 1);
    v_prev_date := v_from_date - 1;


    INSERT INTO logs.dm_calculation_log (
        process_name,
        target_table,
        calc_date,
        status,
        started_at
    )
    VALUES (
        'dm.fill_f101_round_f',
        'dm.dm_f101_round_f',
        i_OnDate,
        'STARTED',
        v_started_at
    )
    RETURNING log_id INTO v_log_id;


    BEGIN

        -- 1.Удаляем старый расчет за этот отчетный период
        DELETE FROM dm.dm_f101_round_f
        WHERE from_date = v_from_date
          AND to_date = v_to_date;

        GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;


        -- 2.Рассчет 101 формы
        INSERT INTO dm.dm_f101_round_f (
            from_date,
            to_date,
            chapter,
            ledger_account,
            characteristic,

            balance_in_rub,
            balance_in_val,
            balance_in_total,

            turn_deb_rub,
            turn_deb_val,
            turn_deb_total,

            turn_cre_rub,
            turn_cre_val,
            turn_cre_total,

            balance_out_rub,
            balance_out_val,
            balance_out_total
        )

        WITH accounts_in_period AS (
            /*
                Берем все счета, которые действовали хотя бы один день
                в отчетном периоде.
            */
            SELECT DISTINCT ON (a.account_rk)
                a.account_rk,
                SUBSTRING(a.account_number FROM 1 FOR 5) AS ledger_account,
                UPPER(TRIM(a.char_type)) AS characteristic,
                TRIM(a.currency_code::TEXT) AS currency_code,

                CASE
                    WHEN TRIM(a.currency_code::TEXT) IN ('810', '643')
                        THEN 1
                    ELSE 0
                END AS is_rub_account

            FROM ds.md_account_d a
            WHERE a.data_actual_date <= v_to_date
              AND COALESCE(a.data_actual_end_date, DATE '5999-12-31') >= v_from_date
              AND a.account_number IS NOT NULL

            ORDER BY
                a.account_rk,
                a.data_actual_date DESC
        ),

        accounts_with_chapter AS (
            SELECT
                a.account_rk,
                a.ledger_account,
                a.characteristic,
                a.currency_code,
                a.is_rub_account,

                COALESCE(l.chapter::TEXT, 'UNKNOWN') AS chapter

            FROM accounts_in_period a

            LEFT JOIN LATERAL (
                SELECT
                    ls.chapter
                FROM ds.md_ledger_account_s ls
                WHERE ls.ledger_account::TEXT = a.ledger_account
                  AND ls.start_date <= v_to_date
                  AND COALESCE(ls.end_date, DATE '5999-12-31') >= v_from_date
                ORDER BY ls.start_date DESC
                LIMIT 1
            ) l ON TRUE
        ),

        balance_in AS (
            SELECT
                b.account_rk,
                SUM(COALESCE(b.balance_out_rub, 0)) AS balance_in_rub_value
            FROM dm.dm_account_balance_f b
            WHERE b.on_date = v_prev_date
            GROUP BY b.account_rk
        ),
/* 
            Исходящий остаток: остаток за последний день отчетного периода.
               Для января 2018 — 31.01.2018. 
*/
               
        balance_out AS (
            SELECT
                b.account_rk,
                SUM(COALESCE(b.balance_out_rub, 0)) AS balance_out_rub_value
            FROM dm.dm_account_balance_f b
            WHERE b.on_date = v_to_date
            GROUP BY b.account_rk
        ),

        turnover AS (
            SELECT
                t.account_rk,

                SUM(COALESCE(t.debet_amount_rub, 0)) AS turn_deb_rub_value,
                SUM(COALESCE(t.credit_amount_rub, 0)) AS turn_cre_rub_value

            FROM dm.dm_account_turnover_f t
            WHERE t.on_date BETWEEN v_from_date AND v_to_date
            GROUP BY t.account_rk
        ),

        prepared AS (
            /*
                Сбор всех показателей конкретного лицевого счета.
            */
            SELECT
                a.chapter,
                a.ledger_account,
                a.characteristic,
                a.is_rub_account,

                COALESCE(bi.balance_in_rub_value, 0) AS balance_in,
                COALESCE(t.turn_deb_rub_value, 0) AS turn_deb,
                COALESCE(t.turn_cre_rub_value, 0) AS turn_cre,
                COALESCE(bo.balance_out_rub_value, 0) AS balance_out

            FROM accounts_with_chapter a

            LEFT JOIN balance_in bi
                ON bi.account_rk = a.account_rk

            LEFT JOIN turnover t
                ON t.account_rk = a.account_rk

            LEFT JOIN balance_out bo
                ON bo.account_rk = a.account_rk
        )

        SELECT
            v_from_date AS from_date,
            v_to_date AS to_date,

            chapter,
            ledger_account,
            characteristic,

            -- Входящие остатки
            SUM(
                CASE
                    WHEN is_rub_account = 1 THEN balance_in
                    ELSE 0
                END
            ) AS balance_in_rub,

            SUM(
                CASE
                    WHEN is_rub_account = 0 THEN balance_in
                    ELSE 0
                END
            ) AS balance_in_val,

            SUM(balance_in) AS balance_in_total,

            -- Дебетовые обороты
            SUM(
                CASE
                    WHEN is_rub_account = 1 THEN turn_deb
                    ELSE 0
                END
            ) AS turn_deb_rub,

            SUM(
                CASE
                    WHEN is_rub_account = 0 THEN turn_deb
                    ELSE 0
                END
            ) AS turn_deb_val,

            SUM(turn_deb) AS turn_deb_total,

            -- Кредитовые обороты
            SUM(
                CASE
                    WHEN is_rub_account = 1 THEN turn_cre
                    ELSE 0
                END
            ) AS turn_cre_rub,

            SUM(
                CASE
                    WHEN is_rub_account = 0 THEN turn_cre
                    ELSE 0
                END
            ) AS turn_cre_val,

            SUM(turn_cre) AS turn_cre_total,

            -- Исходящие остатки
            SUM(
                CASE
                    WHEN is_rub_account = 1 THEN balance_out
                    ELSE 0
                END
            ) AS balance_out_rub,

            SUM(
                CASE
                    WHEN is_rub_account = 0 THEN balance_out
                    ELSE 0
                END
            ) AS balance_out_val,

            SUM(balance_out) AS balance_out_total

        FROM prepared

        GROUP BY
            chapter,
            ledger_account,
            characteristic

        ORDER BY
            chapter,
            ledger_account,
            characteristic;


        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;


       
--логирования
        UPDATE logs.dm_calculation_log
        SET status = 'SUCCESS',
            finished_at = clock_timestamp(),
            duration_seconds = ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at))::NUMERIC, 2),
            rows_deleted = v_rows_deleted,
            rows_inserted = v_rows_inserted,
            error_message = NULL
        WHERE log_id = v_log_id;


    EXCEPTION WHEN OTHERS THEN
        v_error_message := SQLERRM;

        UPDATE logs.dm_calculation_log
        SET status = 'FAILED',
            finished_at = clock_timestamp(),
            duration_seconds = ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at))::NUMERIC, 2),
            rows_deleted = v_rows_deleted,
            rows_inserted = v_rows_inserted,
            error_message = v_error_message
        WHERE log_id = v_log_id;

        RAISE;
    END;
END;
$$;