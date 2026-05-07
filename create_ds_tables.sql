CREATE SCHEMA IF NOT EXISTS ds;
CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
    on_date      date        NOT NULL,
    account_rk   numeric     NOT NULL,
    currency_rk  numeric,
    balance_out  double precision,
    CONSTRAINT pk_ft_balance_f PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f (
    oper_date          date    NOT NULL,
    credit_account_rk  numeric NOT NULL,
    debet_account_rk   numeric NOT NULL,
    credit_amount      double precision,
    debet_amount       double precision
);

CREATE TABLE IF NOT EXISTS ds.md_account_d (
    data_actual_date      date        NOT NULL,
    data_actual_end_date  date        NOT NULL,
    account_rk            numeric     NOT NULL,
    account_number        varchar(20) NOT NULL,
    char_type             varchar(1)  NOT NULL,
    currency_rk           numeric     NOT NULL,
    currency_code         varchar(3)  NOT NULL,
    CONSTRAINT pk_md_account_d PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d (
    currency_rk           numeric    NOT NULL,
    data_actual_date      date       NOT NULL,
    data_actual_end_date  date,
    currency_code         varchar(3),
    code_iso_char         varchar(3),
    CONSTRAINT pk_md_currency_d PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
    data_actual_date      date    NOT NULL,
    data_actual_end_date  date,
    currency_rk           numeric NOT NULL,
    reduced_cource        double precision,
    code_iso_num          varchar(3),
    CONSTRAINT pk_md_exchange_rate_d PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
    chapter                         char(1),
    chapter_name                    varchar(16),
    section_number                  integer,
    section_name                    varchar(22),
    subsection_name                 varchar(21),
    ledger1_account                 integer,
    ledger1_account_name            varchar(47),
    ledger_account                  integer NOT NULL,
    ledger_account_name             varchar(153),
    characteristic                  char(1),
    is_resident                     integer,
    is_reserve                      integer,
    is_reserved                     integer,
    is_loan                         integer,
    is_reserved_assets              integer,
    is_overdue                      integer,
    is_interest                     integer,
    pair_account                    varchar(5),
    start_date                      date NOT NULL,
    end_date                        date,
    is_rub_only                     integer,
    min_term                        varchar(1),
    min_term_measure                varchar(1),
    max_term                        varchar(1),
    max_term_measure                varchar(1),
    ledger_acc_full_name_translit   varchar(1),
    is_revaluation                  varchar(1),
    is_correct                      varchar(1),
    CONSTRAINT pk_md_ledger_account_s PRIMARY KEY (ledger_account, start_date)
);

CREATE TABLE IF NOT EXISTS logs.etl_load_log (
    log_id          bigserial PRIMARY KEY,
    process_name    text NOT NULL,
    target_table    text,
    source_file     text,
    status          text NOT NULL,
    started_at      timestamp NOT NULL DEFAULT now(),
    finished_at     timestamp,
    rows_in_file    integer,
    rows_loaded     integer,
    rows_error      integer,
    error_message   text,
    extra_info      jsonb
);

CREATE INDEX IF NOT EXISTS idx_ft_posting_oper_date
ON ds.ft_posting_f (oper_date);

CREATE INDEX IF NOT EXISTS idx_ft_posting_credit_account
ON ds.ft_posting_f (credit_account_rk);

CREATE INDEX IF NOT EXISTS idx_ft_posting_debet_account
ON ds.ft_posting_f (debet_account_rk);

CREATE INDEX IF NOT EXISTS idx_md_account_actual_period
ON ds.md_account_d (account_rk, data_actual_date, data_actual_end_date);

CREATE INDEX IF NOT EXISTS idx_md_exchange_rate_period
ON ds.md_exchange_rate_d (currency_rk, data_actual_date, data_actual_end_date);

CREATE INDEX IF NOT EXISTS idx_md_ledger_account_period
ON ds.md_ledger_account_s (ledger_account, start_date, end_date);