-- Создание схемы DM, витрин и таблицы логирования расчётов

CREATE SCHEMA IF NOT EXISTS dm;
CREATE SCHEMA IF NOT EXISTS logs;


-- Витрина оборотов по лицевым счетам
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,

    credit_amount NUMERIC(23, 8) DEFAULT 0,
    credit_amount_rub NUMERIC(23, 8) DEFAULT 0,

    debet_amount NUMERIC(23, 8) DEFAULT 0,
    debet_amount_rub NUMERIC(23, 8) DEFAULT 0,

    CONSTRAINT pk_dm_account_turnover_f
        PRIMARY KEY (on_date, account_rk)
);



-- Витрина остатков по лицевым счетам
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,

    balance_out NUMERIC(23, 8) DEFAULT 0,
    balance_out_rub NUMERIC(23, 8) DEFAULT 0,

    CONSTRAINT pk_dm_account_balance_f
        PRIMARY KEY (on_date, account_rk)
);



-- Таблица логирования расчётов DM-витрин
CREATE TABLE IF NOT EXISTS logs.dm_calculation_log (
    log_id BIGSERIAL PRIMARY KEY,

    process_name TEXT NOT NULL,
    target_table TEXT NOT NULL,
    calc_date DATE NOT NULL,

    status TEXT NOT NULL,

    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,

    duration_seconds NUMERIC(12, 2),

    rows_deleted INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,

    error_message TEXT
);


-- Индексы для ускорения проверок и расчётов
CREATE INDEX IF NOT EXISTS idx_dm_account_turnover_f_on_date
ON dm.dm_account_turnover_f (on_date);

CREATE INDEX IF NOT EXISTS idx_dm_account_turnover_f_account_rk
ON dm.dm_account_turnover_f (account_rk);

CREATE INDEX IF NOT EXISTS idx_dm_account_balance_f_on_date
ON dm.dm_account_balance_f (on_date);

CREATE INDEX IF NOT EXISTS idx_dm_account_balance_f_account_rk
ON dm.dm_account_balance_f (account_rk);

CREATE INDEX IF NOT EXISTS idx_dm_calculation_log_calc_date
ON logs.dm_calculation_log (calc_date);

CREATE INDEX IF NOT EXISTS idx_dm_calculation_log_target_table
ON logs.dm_calculation_log (target_table);

CREATE INDEX IF NOT EXISTS idx_dm_calculation_log_process_name
ON logs.dm_calculation_log (process_name);