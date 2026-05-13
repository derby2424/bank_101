import json
import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values


#1.пути проекта


PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"
ENV_FILE = PROJECT_DIR / ".env"



# 2.логирование для airflow
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


def log_partition(title: str) -> None:
    """Визуально разделяет этапы выполнения в логах Airflow."""
    logger.info("-" * 100)
    logger.info(title)
    logger.info("-" * 100)


def log_pipeline_steps(steps: list[str]) -> None:
    logger.info("Пайплайн загрузки:")
    for step_number, step_name in enumerate(steps, start=1):
        logger.info("%s. %s", step_number, step_name)


# 3.подключение к бд
load_dotenv(ENV_FILE)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# 4.кинфигурация таблиц
TABLE_CONFIG = {
    "ft_balance_f": {
        "file": DATA_DIR / "ft_balance_f.csv",
        "target": "ds.ft_balance_f",
        "columns": ["on_date", "account_rk", "currency_rk", "balance_out"],
        "date_columns": ["on_date"],
        "numeric_columns": ["account_rk", "currency_rk", "balance_out"],
        "pk": ["on_date", "account_rk"],
        "truncate_before_load": False,
    },
    "ft_posting_f": {
        "file": DATA_DIR / "ft_posting_f.csv",
        "target": "ds.ft_posting_f",
        "columns": [
            "oper_date",
            "credit_account_rk",
            "debet_account_rk",
            "credit_amount",
            "debet_amount",
        ],
        "date_columns": ["oper_date"],
        "numeric_columns": [
            "credit_account_rk",
            "debet_account_rk",
            "credit_amount",
            "debet_amount",
        ],
        "pk": None,
        "truncate_before_load": True,
    },
    "md_account_d": {
        "file": DATA_DIR / "md_account_d.csv",
        "target": "ds.md_account_d",
        "columns": [
            "data_actual_date",
            "data_actual_end_date",
            "account_rk",
            "account_number",
            "char_type",
            "currency_rk",
            "currency_code",
        ],
        "date_columns": ["data_actual_date", "data_actual_end_date"],
        "numeric_columns": ["account_rk", "currency_rk"],
        "pk": ["data_actual_date", "account_rk"],
        "truncate_before_load": False,
    },
    "md_currency_d": {
        "file": DATA_DIR / "md_currency_d.csv",
        "target": "ds.md_currency_d",
        "columns": [
            "currency_rk",
            "data_actual_date",
            "data_actual_end_date",
            "currency_code",
            "code_iso_char",
        ],
        "date_columns": ["data_actual_date", "data_actual_end_date"],
        "numeric_columns": ["currency_rk"],
        "pk": ["currency_rk", "data_actual_date"],
        "truncate_before_load": False,
    },
    "md_exchange_rate_d": {
        "file": DATA_DIR / "md_exchange_rate_d.csv",
        "target": "ds.md_exchange_rate_d",
        "columns": [
            "data_actual_date",
            "data_actual_end_date",
            "currency_rk",
            "reduced_cource",
            "code_iso_num",
        ],
        "date_columns": ["data_actual_date", "data_actual_end_date"],
        "numeric_columns": ["currency_rk", "reduced_cource"],
        "pk": ["data_actual_date", "currency_rk"],
        "truncate_before_load": False,
    },
    "md_ledger_account_s": {
        "file": DATA_DIR / "md_ledger_account_s.csv",
        "target": "ds.md_ledger_account_s",
        "columns": [
            "chapter",
            "chapter_name",
            "section_number",
            "section_name",
            "subsection_name",
            "ledger1_account",
            "ledger1_account_name",
            "ledger_account",
            "ledger_account_name",
            "characteristic",
            "is_resident",
            "is_reserve",
            "is_reserved",
            "is_loan",
            "is_reserved_assets",
            "is_overdue",
            "is_interest",
            "pair_account",
            "start_date",
            "end_date",
            "is_rub_only",
            "min_term",
            "min_term_measure",
            "max_term",
            "max_term_measure",
            "ledger_acc_full_name_translit",
            "is_revaluation",
            "is_correct",
        ],
        "date_columns": ["start_date", "end_date"],
        "numeric_columns": [
            "section_number",
            "ledger1_account",
            "ledger_account",
            "is_resident",
            "is_reserve",
            "is_reserved",
            "is_loan",
            "is_reserved_assets",
            "is_overdue",
            "is_interest",
            "is_rub_only",
        ],
        "pk": ["ledger_account", "start_date"],
        "truncate_before_load": False,
    },
}

PIPELINE_STEPS = [
    "Подключение к БД",
    "Проверка служебных таблиц logs",
    "Чтение CSV",
    "Очистка и преобразование типов",
    "Отбраковка дублей по бизнес-ключу в карантин",
    "Сбор статистики до загрузки",
    "Загрузка в DS",
    "Сбор статистики после загрузки",
    "Контроль качества и запись итогового лога",
]


# 5. СОЗДАНИЕ СЛУЖЕБНЫХ ТАБЛИЦ LOGS
def ensure_logs_table(conn) -> None:
    """Создаёт схему LOGS и таблицу логов загрузки."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS logs;

            CREATE TABLE IF NOT EXISTS logs.etl_load_log (
                log_id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL,
                process_name TEXT NOT NULL,
                target_table TEXT NOT NULL,
                source_file TEXT,
                status TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                duration_seconds NUMERIC(12, 2),
                log_date DATE NOT NULL DEFAULT CURRENT_DATE,
                rows_in_file INTEGER,
                rows_cleaned INTEGER,
                rows_loaded INTEGER,
                rows_before_load INTEGER,
                rows_after_load INTEGER,
                rows_error INTEGER,
                error_message TEXT
            );

            ALTER TABLE logs.etl_load_log ADD COLUMN IF NOT EXISTS run_id UUID;
            ALTER TABLE logs.etl_load_log ADD COLUMN IF NOT EXISTS duration_seconds NUMERIC(12, 2);
            ALTER TABLE logs.etl_load_log ADD COLUMN IF NOT EXISTS log_date DATE NOT NULL DEFAULT CURRENT_DATE;
            ALTER TABLE logs.etl_load_log ADD COLUMN IF NOT EXISTS rows_cleaned INTEGER;
            ALTER TABLE logs.etl_load_log ADD COLUMN IF NOT EXISTS rows_before_load INTEGER;
            ALTER TABLE logs.etl_load_log ADD COLUMN IF NOT EXISTS rows_after_load INTEGER;

            CREATE INDEX IF NOT EXISTS idx_etl_load_log_log_date
            ON logs.etl_load_log (log_date);

            CREATE INDEX IF NOT EXISTS idx_etl_load_log_run_id
            ON logs.etl_load_log (run_id);

            CREATE INDEX IF NOT EXISTS idx_etl_load_log_target_started
            ON logs.etl_load_log (target_table, started_at);
            """
        )
    conn.commit()
    logger.info("Логовая таблица logs.etl_load_log проверена/создана")


def ensure_rejected_rows_table(conn) -> None:
    """Создаёт таблицу-карантин для отброшенных строк."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS logs;

            CREATE TABLE IF NOT EXISTS logs.etl_rejected_rows (
                reject_id BIGSERIAL PRIMARY KEY,
                log_id BIGINT,
                run_id UUID,
                process_name TEXT NOT NULL,
                target_table TEXT NOT NULL,
                source_file TEXT,
                reject_reason TEXT NOT NULL,
                reject_details TEXT,
                row_number INTEGER,
                raw_data JSONB,
                created_at TIMESTAMP NOT NULL DEFAULT now(),
                reject_date DATE NOT NULL DEFAULT CURRENT_DATE
            );

            ALTER TABLE logs.etl_rejected_rows ADD COLUMN IF NOT EXISTS run_id UUID;
            ALTER TABLE logs.etl_rejected_rows ADD COLUMN IF NOT EXISTS reject_date DATE NOT NULL DEFAULT CURRENT_DATE;

            CREATE INDEX IF NOT EXISTS idx_etl_rejected_rows_reject_date
            ON logs.etl_rejected_rows (reject_date);

            CREATE INDEX IF NOT EXISTS idx_etl_rejected_rows_run_id
            ON logs.etl_rejected_rows (run_id);

            CREATE INDEX IF NOT EXISTS idx_etl_rejected_rows_target_reason
            ON logs.etl_rejected_rows (target_table, reject_reason);
            """
        )
    conn.commit()
    logger.info("Таблица карантина logs.etl_rejected_rows проверена/создана")


# 6. ЛОГИРОВАНИЕ В logs.etl_load_log
def log_start(
    conn,
    run_id: uuid.UUID,
    process_name: str,
    target_table: str,
    source_file,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO logs.etl_load_log (
                run_id,
                process_name,
                target_table,
                source_file,
                status,
                started_at,
                log_date
            )
            VALUES (%s, %s, %s, %s, 'STARTED', now(), CURRENT_DATE)
            RETURNING log_id;
            """,
            (str(run_id), process_name, target_table, str(source_file)),
        )
        log_id = cur.fetchone()[0]
    conn.commit()
    return log_id


def log_success(
    conn,
    log_id: int,
    rows_in_file: int,
    rows_cleaned: int,
    rows_loaded: int,
    rows_before_load: int,
    rows_after_load: int,
    rows_error: int,
    duration_seconds: float,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE logs.etl_load_log
            SET status = 'SUCCESS',
                finished_at = now(),
                rows_in_file = %s,
                rows_cleaned = %s,
                rows_loaded = %s,
                rows_before_load = %s,
                rows_after_load = %s,
                rows_error = %s,
                duration_seconds = %s,
                error_message = NULL
            WHERE log_id = %s;
            """,
            (
                rows_in_file,
                rows_cleaned,
                rows_loaded,
                rows_before_load,
                rows_after_load,
                rows_error,
                duration_seconds,
                log_id,
            ),
        )
    conn.commit()


def log_failed(
    conn,
    log_id: int,
    error_message,
    rows_in_file=None,
    rows_cleaned=None,
    rows_loaded=None,
    rows_before_load=None,
    rows_after_load=None,
    rows_error=None,
    duration_seconds=None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE logs.etl_load_log
            SET status = 'FAILED',
                finished_at = now(),
                rows_in_file = %s,
                rows_cleaned = %s,
                rows_loaded = %s,
                rows_before_load = %s,
                rows_after_load = %s,
                rows_error = %s,
                duration_seconds = %s,
                error_message = %s
            WHERE log_id = %s;
            """,
            (
                rows_in_file,
                rows_cleaned,
                rows_loaded,
                rows_before_load,
                rows_after_load,
                rows_error,
                duration_seconds,
                str(error_message),
                log_id,
            ),
        )
    conn.commit()

# 7. карантин 
def send_rejected_rows_to_quarantine(
    conn,
    log_id: int,
    run_id: uuid.UUID,
    process_name: str,
    target_table: str,
    source_file,
    rejected_df: pd.DataFrame,
    reject_reason: str,
    reject_details: str,
) -> int:
    """Сохраняет отброшенные строки в logs.etl_rejected_rows."""
    if rejected_df is None or rejected_df.empty:
        logger.info(
            f"Нет строк для карантина: "
            f"target_table={target_table}, reason={reject_reason}"
        )
        return 0

    quarantine_rows = []

    for dataframe_index, row in rejected_df.iterrows():
        raw_data = row.where(pd.notnull(row), None).to_dict()

        # raw_data сохраняется в колонку JSONB.
        # psycopg2 не умеет автоматически адаптировать обычный dict, поэтому оборачиваем словарь в Json.
        # default=str нужен для дат, Decimal/numpy-типов и других значений, которые json.dumps не сериализует напрямую.
        raw_data_json = Json(
            raw_data,
            dumps=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                default=str,
            ),
        )

        quarantine_rows.append(
            (
                log_id,
                str(run_id),
                process_name,
                target_table,
                str(source_file),
                reject_reason,
                reject_details,
                int(dataframe_index) + 2,
                raw_data_json,
            )
        )

    sql = """
        INSERT INTO logs.etl_rejected_rows (
            log_id,
            run_id,
            process_name,
            target_table,
            source_file,
            reject_reason,
            reject_details,
            row_number,
            raw_data
        )
        VALUES %s;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, quarantine_rows, page_size=1000)

    conn.commit()

    logger.warning(
        f"В карантин отправлено строк: {len(quarantine_rows)}. "
        f"target_table={target_table}, reason={reject_reason}"
    )

    return len(quarantine_rows)


def reject_duplicates_by_pk(
    conn,
    log_id: int,
    run_id: uuid.UUID,
    process_name: str,
    target_table: str,
    source_file,
    df: pd.DataFrame,
    pk_columns: list[str] | None,
):
    """
    Отбрасывает дубли по бизнес-ключу в карантин.
    В основную загрузку оставляется последняя строка по ключу.
    """
    if not pk_columns:
        logger.info(f"Для {target_table} ключ не задан, дедубликация не выполняется")
        return df, 0

    missing_pk_columns = [col for col in pk_columns if col not in df.columns]

    duplicate_mask = df.duplicated(subset=pk_columns, keep="last")
    rejected_df = df[duplicate_mask].copy()
    cleaned_df = df.drop_duplicates(subset=pk_columns, keep="last").copy()

    rejected_count = send_rejected_rows_to_quarantine(
        conn=conn,
        log_id=log_id,
        run_id=run_id,
        process_name=process_name,
        target_table=target_table,
        source_file=source_file,
        rejected_df=rejected_df,
        reject_reason="DUPLICATE_BY_PK",
        reject_details=(
            f"Обнаружены дубли по ключу {pk_columns}. "
            f"В загрузку оставлена последняя строка по каждому ключу."
        ),
    )

    logger.info(
        f"Дедубликация по ключу для {target_table}: "
        f"до={len(df)}, после={len(cleaned_df)}, "
        f"в карантин={rejected_count}"
    )

    return cleaned_df, rejected_count


def reject_rows_with_missing_pk(
    conn,
    log_id: int,
    run_id: uuid.UUID,
    process_name: str,
    target_table: str,
    source_file,
    df: pd.DataFrame,
    pk_columns: list[str] | None,
):
    """Отбрасывает строки с пустыми значениями бизнес-ключа в карантин."""
    if not pk_columns:
        return df, 0

    missing_pk_mask = df[pk_columns].isna().any(axis=1)
    rejected_df = df[missing_pk_mask].copy()
    cleaned_df = df[~missing_pk_mask].copy()

    rejected_count = send_rejected_rows_to_quarantine(
        conn=conn,
        log_id=log_id,
        run_id=run_id,
        process_name=process_name,
        target_table=target_table,
        source_file=source_file,
        rejected_df=rejected_df,
        reject_reason="MISSING_PK",
        reject_details=(
            f"В строке отсутствует значение одной из ключевых колонок {pk_columns}. "
            f"Строка не передана в целевую таблицу."
        ),
    )

    logger.info(
        f"Проверка пустых ключей для {target_table}: "
        f"до={len(df)}, после={len(cleaned_df)}, "
        f"в карантин={rejected_count}"
    )

    return cleaned_df, rejected_count


# 8. ПРЕОБРАЗОВАНИЕ ЗНАЧЕНИЙ
def normalize_column_name(column_name: str) -> str:
    return str(column_name).strip().lower().replace(" ", "_").replace("\ufeff", "")


def parse_date(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "" or value.lower() in ("nan", "none", "null"):
        return None

    date_formats = [
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%Y.%m.%d",
    ]

    for date_format in date_formats:
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            continue

    raise ValueError(f"Не удалось распознать дату: {value}")


def parse_number(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "" or value.lower() in ("nan", "none", "null"):
        return None

    value = value.replace(" ", "").replace(",", ".")
    return float(value)


def clean_text(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "" or value.lower() in ("nan", "none", "null"):
        return None

    return value


# 9. ЧТЕНИЕ CSV
def read_csv_file(file_path: Path) -> pd.DataFrame:

    encodings = ["utf-8-sig", "utf-8", "cp1251", "windows-1251", "latin1"]
    separators = [";", ","]
    last_error = None

    for encoding in encodings:
        for separator in separators:
            try:
                df = pd.read_csv(
                    file_path,
                    sep=separator,
                    dtype=str,
                    encoding=encoding,
                )


                df.columns = [normalize_column_name(col) for col in df.columns]

                logger.info(
                    f"Файл {file_path.name} прочитан: "
                    f"encoding={encoding}, sep='{separator}', "
                    f"строк={len(df)}, колонок={len(df.columns)}"
                )

                return df

            except UnicodeDecodeError as error:
                last_error = error
                continue
            except Exception as error:
                last_error = error
                continue

    raise ValueError(
        f"Не удалось прочитать CSV-файл {file_path}. "
        f"Последняя ошибка: {last_error}"
    )


# 10. ОЧИСТКА И ТРАНСФОРМАЦИЯ DATAFRAME
def transform_dataframe(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, int]:
    """
    Приводит DataFrame к структуре целевой DS-таблицы.
    Дубли по ключу здесь не удаляются, они обрабатываются отдельно.
    """
    required_columns = config["columns"]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.warning(
            f"В CSV отсутствуют колонки: {missing_columns}. "
            f"Они будут добавлены со значением NULL."
        )
        for column in missing_columns:
            df[column] = None

    df = df[required_columns].copy()

    rows_before_drop_empty = len(df)
    df = df.dropna(how="all")
    rows_after_drop_empty = len(df)
    rows_dropped_empty = rows_before_drop_empty - rows_after_drop_empty

    for column in config["date_columns"]:
        df[column] = df[column].apply(parse_date)

    for column in config["numeric_columns"]:
        df[column] = df[column].apply(parse_number)

    text_columns = [
        col
        for col in config["columns"]
        if col not in config["date_columns"] and col not in config["numeric_columns"]
    ]

    for column in text_columns:
        df[column] = df[column].apply(clean_text)

    df = df.where(pd.notnull(df), None)

    return df, rows_dropped_empty


# 11. СТАТИСТИКА
def collect_dataframe_stats(df: pd.DataFrame) -> dict:
    return {
        "rows_count": len(df),
        "columns_count": len(df.columns),
        "columns": list(df.columns),
        "nulls_by_column": df.isna().sum().to_dict(),
        "full_duplicates": int(df.duplicated().sum()),
    }


def log_dataframe_stats(target_table: str, stats: dict) -> None:
    logger.info(f"Статистика DataFrame для {target_table}")
    logger.info(f"Количество строк: {stats['rows_count']}")
    logger.info(f"Количество колонок: {stats['columns_count']}")
    logger.info(f"Колонки: {stats['columns']}")
    logger.info(f"дубли строк: {stats['full_duplicates']}")
    logger.info(f"NULL по колонкам: {stats['nulls_by_column']}")


def collect_db_stats(conn, target_table: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {target_table};")
        rows_count = cur.fetchone()[0]
    return {"rows_count": rows_count}


def log_db_stats(target_table: str, stats: dict) -> None:
    logger.info(f"Статистика БД для {target_table}")
    logger.info(f"Количество строк в таблице: {stats['rows_count']}")


def count_existing_keys(conn, target_table: str, df: pd.DataFrame, pk_columns: list[str]) -> int:
    """Считает, сколько ключей из DataFrame уже есть в целевой таблице."""
    if not pk_columns or df.empty:
        return 0

    unique_keys = df[pk_columns].drop_duplicates()
    where_sql = " AND ".join([f"{column} = %s" for column in pk_columns])
    sql = f"SELECT 1 FROM {target_table} WHERE {where_sql} LIMIT 1;"

    existing_keys = 0
    with conn.cursor() as cur:
        for key_values in unique_keys.itertuples(index=False, name=None):
            cur.execute(sql, key_values)
            if cur.fetchone() is not None:
                existing_keys += 1

    return existing_keys


# 12. SQL ДЛЯ ЗАГРУЗКИ
def build_insert_sql(target_table: str, columns: list[str]) -> str:
    columns_sql = ", ".join(columns)
    return f"""
        INSERT INTO {target_table} ({columns_sql})
        VALUES %s;
    """


def build_upsert_sql(target_table: str, columns: list[str], pk_columns: list[str]) -> str:
    columns_sql = ", ".join(columns)
    conflict_sql = ", ".join(pk_columns)
    update_columns = [col for col in columns if col not in pk_columns]
    update_sql = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])

    return f"""
        INSERT INTO {target_table} ({columns_sql})
        VALUES %s
        ON CONFLICT ({conflict_sql})
        DO UPDATE SET {update_sql};
    """


# 13. КОНТРОЛЬ КАЧЕСТВА
def quality_check(
    target_table: str,
    rows_in_file: int,
    rows_cleaned: int,
    rows_loaded: int,
    rows_rejected: int,
    rows_dropped_empty: int,
    rows_before_load: int,
    rows_after_load: int,
    rows_expected_after_load: int,
    keys_in_source: int,
    keys_found_after_load: int,
    truncate_before_load: bool,
) -> None:
    logger.info(f"Контроль качества для {target_table}")

    accounted_rows = rows_cleaned + rows_rejected + rows_dropped_empty
    if rows_in_file != accounted_rows:
        raise ValueError(
            f"строк в файле {rows_in_file}, "
            f"(к загрузке={rows_cleaned}, карантин={rows_rejected}, "
            f"пустые={rows_dropped_empty})"
        )

    if rows_cleaned != rows_loaded:
        raise ValueError(
            f"Ошибка контроля качества для {target_table}: "
            f"строк после очистки {rows_cleaned}, "
            f"а передано на загрузку {rows_loaded}"
        )

    if truncate_before_load:
        if rows_after_load != rows_loaded:
            raise ValueError(
                f"Ошибка контроля качества для {target_table}: "
                f"загружено {rows_loaded}, "
                f"в БД после загрузки {rows_after_load}"
            )
    else:
        if rows_after_load != rows_expected_after_load:
            raise ValueError(
                f"Ошибка контроля качества для {target_table}: "
                f"ожидалось строк {rows_expected_after_load}, "
                f"фактически строк {rows_after_load}. "
                f"До загрузки было {rows_before_load}, "
                f"к загрузке передано {rows_loaded}"
            )

        if keys_found_after_load != keys_in_source:
            raise ValueError(
                f"Ошибка контроля качества для {target_table}: "
                f"после загрузки найдено ключей {keys_found_after_load} "
                f"из {keys_in_source} ключей текущего файла"
            )

    logger.info(
        f"Контроль качества пройден для {target_table}. "
        f"rows_in_file={rows_in_file}, "
        f"rows_cleaned={rows_cleaned}, "
        f"rows_loaded={rows_loaded}, "
        f"rows_before_load={rows_before_load}, "
        f"rows_after_load={rows_after_load}, "
        f"keys_found_after_load={keys_found_after_load}"
    )

# 14. ЗАГРУЗКА ОДНОЙ ТАБЛИЦЫ
def load_table(conn, run_id: uuid.UUID, table_name: str, config: dict) -> None:
    process_name = "load_csv_to_ds"
    target_table = config["target"]
    source_file = config["file"]
    pk_columns = config.get("pk")

    rows_in_file = 0
    rows_cleaned = 0
    rows_loaded = 0
    rows_before_load = 0
    rows_after_load = 0
    rows_rejected = 0
    rows_error = 0
    rows_dropped_empty = 0
    rows_expected_after_load = 0
    keys_in_source = 0
    keys_found_after_load = 0

    table_start_time = time.time()

    log_partition(f"НАЧАЛО ЗАГРУЗКИ: {target_table}")

    log_id = log_start(
        conn=conn,
        run_id=run_id,
        process_name=process_name,
        target_table=target_table,
        source_file=source_file,
    )

    logger.info(f"В logs.etl_load_log создана запись STARTED, log_id={log_id}")
    logger.info(f"run_id={run_id}")
    logger.info("Пауза 5 секунд")
    time.sleep(5)

    try:
        log_partition(f"ЧТЕНИЕ CSV: {source_file.name}")
        df_raw = read_csv_file(source_file)
        rows_in_file = len(df_raw)
        logger.info(f"Строк в CSV: {rows_in_file}")

        log_partition(f"ОЧИСТКА И ПРЕОБРАЗОВАНИЕ: {target_table}")
        df, rows_dropped_empty = transform_dataframe(df_raw, config)
        logger.info(f"Строк после очистки: {len(df)}")

        log_partition(f"КАРАНТИН ДУБЛЕЙ ПО КЛЮЧУ: {target_table}")
        df, missing_pk_rejected_count = reject_rows_with_missing_pk(
            conn=conn,
            log_id=log_id,
            run_id=run_id,
            process_name=process_name,
            target_table=target_table,
            source_file=source_file,
            df=df,
            pk_columns=pk_columns,
        )
        df, duplicate_rejected_count = reject_duplicates_by_pk(
            conn=conn,
            log_id=log_id,
            run_id=run_id,
            process_name=process_name,
            target_table=target_table,
            source_file=source_file,
            df=df,
            pk_columns=pk_columns,
        )
        rows_rejected += missing_pk_rejected_count
        rows_rejected += duplicate_rejected_count
        rows_cleaned = len(df)
        logger.info(f"Строк после карантина дублей: {rows_cleaned}")

        log_partition(f"СТАТИСТИКА ПЕРЕД ЗАГРУЗКОЙ: {target_table}")
        df_stats = collect_dataframe_stats(df)
        log_dataframe_stats(target_table, df_stats)

        log_partition(f"СТАТИСТИКА БД ДО ЗАГРУЗКИ: {target_table}")
        db_stats_before = collect_db_stats(conn, target_table)
        rows_before_load = db_stats_before["rows_count"]
        log_db_stats(target_table, db_stats_before)

        if pk_columns:
            keys_in_source = len(df[pk_columns].drop_duplicates())
            existing_keys_before = count_existing_keys(conn, target_table, df, pk_columns)
            rows_expected_after_load = rows_before_load + keys_in_source - existing_keys_before
            logger.info(
                f"Проверка ключей для {target_table}: "
                f"ключей в файле={keys_in_source}, "
                f"уже есть в БД={existing_keys_before}"
            )
        else:
            rows_expected_after_load = rows_cleaned

        log_partition(f"ЭТАП 6. ЗАГРУЗКА В БД: {target_table}")
        rows = [tuple(row) for row in df.to_numpy()]
        rows_loaded = len(rows)

        with conn.cursor() as cur:
            if config["truncate_before_load"]:
                logger.info(f"Очистка таблицы перед загрузкой: {target_table}")
                cur.execute(f"TRUNCATE TABLE {target_table};")

            if not rows:
                logger.warning(f"Файл пустой после очистки: {source_file}")
            else:
                if pk_columns is None:
                    logger.info(f"Для {target_table} используется INSERT")
                    sql = build_insert_sql(
                        target_table=target_table,
                        columns=config["columns"],
                    )
                else:
                    logger.info(f"Для {target_table} выполняется загрузка по ключу: {pk_columns}")
                    sql = build_upsert_sql(
                        target_table=target_table,
                        columns=config["columns"],
                        pk_columns=pk_columns,
                    )

                execute_values(cur, sql, rows, page_size=1000)

        logger.info(f"В таблицу {target_table} передано строк: {rows_loaded}")

        log_partition(f"СТАТИСТИКА БД ПОСЛЕ ЗАГРУЗКИ: {target_table}")
        db_stats_after = collect_db_stats(conn, target_table)
        rows_after_load = db_stats_after["rows_count"]
        log_db_stats(target_table, db_stats_after)

        if pk_columns:
            keys_found_after_load = count_existing_keys(conn, target_table, df, pk_columns)
        else:
            keys_found_after_load = rows_after_load

        log_partition(f"КОНТРОЛЬ КАЧЕСТВА: {target_table}")
        quality_check(
            target_table=target_table,
            rows_in_file=rows_in_file,
            rows_cleaned=rows_cleaned,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            rows_dropped_empty=rows_dropped_empty,
            rows_before_load=rows_before_load,
            rows_after_load=rows_after_load,
            rows_expected_after_load=rows_expected_after_load,
            keys_in_source=keys_in_source,
            keys_found_after_load=keys_found_after_load,
            truncate_before_load=config["truncate_before_load"],
        )

        conn.commit()

        rows_error = 0
        table_duration = round(time.time() - table_start_time, 2)

        log_success(
            conn=conn,
            log_id=log_id,
            rows_in_file=rows_in_file,
            rows_cleaned=rows_cleaned,
            rows_loaded=rows_loaded,
            rows_before_load=rows_before_load,
            rows_after_load=rows_after_load,
            rows_error=rows_error,
            duration_seconds=table_duration,
        )

        logger.info(
            f"SUCCESS: {target_table} загружена успешно. "
            f"Время выполнения: {table_duration} секунд"
        )

    except Exception as error:
        conn.rollback()
        rows_error = 1

        log_failed(
            conn=conn,
            log_id=log_id,
            error_message=error,
            rows_in_file=rows_in_file,
            rows_cleaned=rows_cleaned,
            rows_loaded=rows_loaded,
            rows_before_load=rows_before_load,
            rows_after_load=rows_after_load,
            rows_error=rows_error,
            duration_seconds=round(time.time() - table_start_time, 2),
        )

        logger.exception(f"FAILED: ошибка при загрузке {target_table}: {error}")
        raise


# 15. ОСНОВНАЯ ФУНКЦИЯ MAIN
def main() -> None:
    run_id = uuid.uuid4()
    pipeline_start_time = time.time()

    log_partition("СТАРТ ETL-ПРОЦЕССА ЗАГРУЗКИ CSV В СЛОЙ DS")
    logger.info(f"run_id={run_id}")
    log_pipeline_steps(PIPELINE_STEPS)

    logger.info(f"PROJECT_DIR = {PROJECT_DIR}")
    logger.info(f"DATA_DIR = {DATA_DIR}")
    logger.info(f"ENV_FILE = {ENV_FILE}")

    conn = get_connection()

    try:
        logger.info("Соединение с БД установлено")

        ensure_logs_table(conn)
        ensure_rejected_rows_table(conn)

        for table_name, config in TABLE_CONFIG.items():
            load_table(
                conn=conn,
                run_id=run_id,
                table_name=table_name,
                config=config,
            )

        pipeline_duration = round(time.time() - pipeline_start_time, 2)

        log_partition("ETL-ПРОЦЕСС УСПЕШНО ЗАВЕРШЁН")
        logger.info(f"Общее время выполнения ETL: {pipeline_duration} секунд")

    except Exception as error:
        pipeline_duration = round(time.time() - pipeline_start_time, 2)

        log_partition("ETL-ПРОЦЕСС ЗАВЕРШЁН С ОШИБКОЙ")
        logger.exception(f"Ошибка выполнения ETL: {error}")
        logger.info(f"Время до ошибки: {pipeline_duration} секунд")

        raise

    finally:
        conn.close()
        logger.info("Соединение с БД закрыто")


if __name__ == "__main__":
    main()
