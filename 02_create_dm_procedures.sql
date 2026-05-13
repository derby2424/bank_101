DROP PROCEDURE IF EXISTS ds.fill_account_turnover_f(DATE);

CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id BIGINT;
    v_started_at TIMESTAMP;
    v_rows_deleted INTEGER := 0;
    v_rows_inserted INTEGER := 0;
    v_error_message TEXT;
BEGIN
    v_started_at := clock_timestamp();

    INSERT INTO logs.dm_calculation_log (
        process_name,
        target_table,
        calc_date,
        status,
        started_at
    )
    VALUES (
        'ds.fill_account_turnover_f',
        'dm.dm_account_turnover_f',
        i_OnDate,
        'STARTED',
        v_started_at
    )
    RETURNING log_id INTO v_log_id;


    BEGIN
        -- Для возможности многократного перезапуска расчёта удаляем данные за дату расчёта.
        DELETE FROM dm.dm_account_turnover_f
        WHERE on_date = i_OnDate;

        GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;

        -- Расчёт оборотов.
        -- Если счёт кредитовый, сумма попадает в credit_amount.
        -- Если счёт дебетовый, сумма попадает в debet_amount.
        INSERT INTO dm.dm_account_turnover_f (
            on_date,
            account_rk,
            credit_amount,
            credit_amount_rub,
            debet_amount,
            debet_amount_rub
        )
        WITH turnover_raw AS (
            SELECT
                p.credit_account_rk AS account_rk,
                SUM(COALESCE(p.credit_amount, 0)) AS credit_amount,
                0::NUMERIC AS debet_amount
            FROM ds.ft_posting_f p
            WHERE p.oper_date = i_OnDate
              AND p.credit_account_rk IS NOT NULL
            GROUP BY p.credit_account_rk

            UNION ALL

            SELECT
                p.debet_account_rk AS account_rk,
                0::NUMERIC AS credit_amount,
                SUM(COALESCE(p.debet_amount, 0)) AS debet_amount
            FROM ds.ft_posting_f p
            WHERE p.oper_date = i_OnDate
              AND p.debet_account_rk IS NOT NULL
            GROUP BY p.debet_account_rk
        ),

        turnover_grouped AS (
            SELECT
                account_rk,
                SUM(credit_amount) AS credit_amount,
                SUM(debet_amount) AS debet_amount
            FROM turnover_raw
            GROUP BY account_rk
        )

        SELECT
            i_OnDate AS on_date,
            t.account_rk,

            COALESCE(t.credit_amount, 0) AS credit_amount,
            COALESCE(t.credit_amount, 0) * COALESCE(rate.reduced_cource, 1) AS credit_amount_rub,

            COALESCE(t.debet_amount, 0) AS debet_amount,
            COALESCE(t.debet_amount, 0) * COALESCE(rate.reduced_cource, 1) AS debet_amount_rub

        FROM turnover_grouped t

        -- Определяем валюту счёта на дату расчёта.
        LEFT JOIN LATERAL (
            SELECT
                a.currency_rk
            FROM ds.md_account_d a
            WHERE a.account_rk = t.account_rk
              AND i_OnDate BETWEEN a.data_actual_date
                               AND COALESCE(a.data_actual_end_date, DATE '5999-12-31')
            ORDER BY a.data_actual_date DESC
            LIMIT 1
        ) acc ON TRUE

        -- Определяем курс валюты на дату расчёта.
        LEFT JOIN LATERAL (
            SELECT
                er.reduced_cource
            FROM ds.md_exchange_rate_d er
            WHERE er.currency_rk = acc.currency_rk
              AND i_OnDate BETWEEN er.data_actual_date
                               AND COALESCE(er.data_actual_end_date, DATE '5999-12-31')
            ORDER BY er.data_actual_date DESC
            LIMIT 1
        ) rate ON TRUE;

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;


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


-- Процедура расчёта витрины остатков
DROP PROCEDURE IF EXISTS ds.fill_account_balance_f(DATE);

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id BIGINT;
    v_started_at TIMESTAMP;
    v_rows_deleted INTEGER := 0;
    v_rows_inserted INTEGER := 0;
    v_error_message TEXT;
BEGIN
    v_started_at := clock_timestamp();

    INSERT INTO logs.dm_calculation_log (
        process_name,
        target_table,
        calc_date,
        status,
        started_at
    )
    VALUES (
        'ds.fill_account_balance_f',
        'dm.dm_account_balance_f',
        i_OnDate,
        'STARTED',
        v_started_at
    )
    RETURNING log_id INTO v_log_id;


    BEGIN
        DELETE FROM dm.dm_account_balance_f
        WHERE on_date = i_OnDate;

        GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;

        -- Расчёт остатков.
        -- Берём все счета, действующие на дату расчёта. Остаток считаем от предыдущего дня и оборотов текущего дня.
        INSERT INTO dm.dm_account_balance_f (
            on_date,
            account_rk,
            balance_out,
            balance_out_rub
        )
        WITH actual_accounts AS (
            SELECT DISTINCT ON (a.account_rk)
                a.account_rk,
                a.char_type
            FROM ds.md_account_d a
            WHERE i_OnDate BETWEEN a.data_actual_date
                               AND COALESCE(a.data_actual_end_date, DATE '5999-12-31')
            ORDER BY a.account_rk, a.data_actual_date DESC
        )

        SELECT
            i_OnDate AS on_date,
            a.account_rk,

            CASE
                -- Активный счёт: остаток сегодня = остаток вчера + дебет - кредит
                WHEN UPPER(TRIM(a.char_type)) IN ('А', 'A') THEN
                    COALESCE(prev.balance_out, 0)
                    + COALESCE(t.debet_amount, 0)
                    - COALESCE(t.credit_amount, 0)

                -- Пассивный счёт: остаток сегодня = остаток вчера - дебет + кредит
                WHEN UPPER(TRIM(a.char_type)) IN ('П', 'P') THEN
                    COALESCE(prev.balance_out, 0)
                    - COALESCE(t.debet_amount, 0)
                    + COALESCE(t.credit_amount, 0)

                ELSE
                    COALESCE(prev.balance_out, 0)
            END AS balance_out,

            CASE
                -- Активный счёт в рублях:
                WHEN UPPER(TRIM(a.char_type)) IN ('А', 'A') THEN
                    COALESCE(prev.balance_out_rub, 0)
                    + COALESCE(t.debet_amount_rub, 0)
                    - COALESCE(t.credit_amount_rub, 0)

                -- Пассивный счёт в рублях:
                WHEN UPPER(TRIM(a.char_type)) IN ('П', 'P') THEN
                    COALESCE(prev.balance_out_rub, 0)
                    - COALESCE(t.debet_amount_rub, 0)
                    + COALESCE(t.credit_amount_rub, 0)

                ELSE
                    COALESCE(prev.balance_out_rub, 0)
            END AS balance_out_rub

        FROM actual_accounts a

        LEFT JOIN dm.dm_account_balance_f prev
            ON prev.account_rk = a.account_rk
           AND prev.on_date = i_OnDate - 1

        LEFT JOIN dm.dm_account_turnover_f t
            ON t.account_rk = a.account_rk
           AND t.on_date = i_OnDate;

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;


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
