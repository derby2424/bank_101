-- 1. Контроль до расчёта
SELECT
    'BEFORE TURNOVER CALCULATION' AS check_point,
    COUNT(*) AS rows_count
FROM dm.dm_account_turnover_f;


SELECT
    'BEFORE BALANCE CALCULATION' AS check_point,
    COUNT(*) AS rows_count
FROM dm.dm_account_balance_f;


-- 2. Расчёт витрины оборотов за каждый день января 2018
DO $$
DECLARE
    v_date DATE;
BEGIN
    FOR v_date IN
        SELECT generate_series(
            DATE '2018-01-01',
            DATE '2018-01-31',
            INTERVAL '1 day'
        )::DATE
    LOOP
        RAISE NOTICE 'Расчёт витрины оборотов за дату: %', v_date;

        CALL ds.fill_account_turnover_f(v_date);
    END LOOP;
END $$;


-- 4.заполнение витрины остатков за 31.12.2017
DO $$
DECLARE
    v_log_id BIGINT;
    v_started_at TIMESTAMP;
    v_rows_deleted INTEGER := 0;
    v_rows_inserted INTEGER := 0;
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
        'initial_load_balance_2017_12_31',
        'dm.dm_account_balance_f',
        DATE '2017-12-31',
        'STARTED',
        v_started_at
    )
    RETURNING log_id INTO v_log_id;


    DELETE FROM dm.dm_account_balance_f
    WHERE on_date = DATE '2017-12-31';

    GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;


    INSERT INTO dm.dm_account_balance_f (
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    )
    SELECT
        b.on_date,
        b.account_rk,
        b.balance_out,
        b.balance_out * COALESCE(rate.reduced_cource, 1) AS balance_out_rub
    FROM ds.ft_balance_f b

    LEFT JOIN LATERAL (
        SELECT
            er.reduced_cource
        FROM ds.md_exchange_rate_d er
        WHERE er.currency_rk = b.currency_rk
          AND DATE '2017-12-31' BETWEEN er.data_actual_date
                                   AND COALESCE(er.data_actual_end_date, DATE '5999-12-31')
        ORDER BY er.data_actual_date DESC
        LIMIT 1
    ) rate ON TRUE

    WHERE b.on_date = DATE '2017-12-31';

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;


    UPDATE logs.dm_calculation_log
    SET status = 'SUCCESS',
        finished_at = clock_timestamp(),
        duration_seconds = ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at))::NUMERIC, 2),
        rows_deleted = v_rows_deleted,
        rows_inserted = v_rows_inserted,
        error_message = NULL
    WHERE log_id = v_log_id;

    RAISE NOTICE 'Начальные остатки за 31.12.2017 загружены. Вставлено строк: %', v_rows_inserted;

EXCEPTION WHEN OTHERS THEN
    UPDATE logs.dm_calculation_log
    SET status = 'FAILED',
        finished_at = clock_timestamp(),
        duration_seconds = ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at))::NUMERIC, 2),
        rows_deleted = v_rows_deleted,
        rows_inserted = v_rows_inserted,
        error_message = SQLERRM
    WHERE log_id = v_log_id;

    RAISE;
END $$;


-- 6. Расчёт витрины остатков за каждый день января 2018
DO $$
DECLARE
    v_date DATE;
BEGIN
    FOR v_date IN
        SELECT generate_series(
            DATE '2018-01-01',
            DATE '2018-01-31',
            INTERVAL '1 day'
        )::DATE
    LOOP
        RAISE NOTICE 'Расчёт витрины остатков за дату: %', v_date;

        CALL ds.fill_account_balance_f(v_date);
    END LOOP;
END $$;



--выбирается любой счёт по которому есть остатки за январь.
WITH sample_account AS (
    SELECT account_rk
    FROM dm.dm_account_balance_f
    WHERE on_date BETWEEN DATE '2018-01-01' AND DATE '2018-01-31'
    GROUP BY account_rk
    ORDER BY COUNT(*) DESC
    LIMIT 1
)
SELECT
    b.on_date,
    b.account_rk,
    b.balance_out,
    b.balance_out_rub,
    COALESCE(t.debet_amount, 0) AS debet_amount,
    COALESCE(t.credit_amount, 0) AS credit_amount,
    COALESCE(t.debet_amount_rub, 0) AS debet_amount_rub,
    COALESCE(t.credit_amount_rub, 0) AS credit_amount_rub
FROM dm.dm_account_balance_f b
JOIN sample_account s
    ON s.account_rk = b.account_rk
LEFT JOIN dm.dm_account_turnover_f t
    ON t.account_rk = b.account_rk
   AND t.on_date = b.on_date
WHERE b.on_date BETWEEN DATE '2017-12-31' AND DATE '2018-01-31'
ORDER BY b.on_date;
