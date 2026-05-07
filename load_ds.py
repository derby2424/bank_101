import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values



#1. Пути проекта
PROJECT_DIR = Path(__file__).resolve().parents[1] #возвращает полный путь к скрипту, поднимает на два уровня в дир
DATA_DIR = PROJECT_DIR / "data"
ENV_FILE = PROJECT_DIR / ".env"



# 2. Загрузка переменных окружения
load_dotenv(ENV_FILE)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


# 3. Конфигурация таблиц
TABLE_CONFIG = {
    "ft_balance_f": {
        "file": DATA_DIR / "ft_balance_f.csv",
        "target": "ds.ft_balance_f",
        "columns": [
            "on_date",
            "account_rk",
            "currency_rk",
            "balance_out",
        ],
        "date_columns": [
            "on_date",
        ],
        "numeric_columns": [
            "account_rk",
            "currency_rk",
            "balance_out",
        ],
        "pk": [
            "on_date",
            "account_rk",
        ],
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
        "date_columns": [
            "oper_date",
        ],
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
        "date_columns": [
            "data_actual_date",
            "data_actual_end_date",
        ],
        "numeric_columns": [
            "account_rk",
            "currency_rk",
        ],
        "pk": [
            "data_actual_date",
            "account_rk",
        ],
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
        "date_columns": [
            "data_actual_date",
            "data_actual_end_date",
        ],
        "numeric_columns": [
            "currency_rk",
        ],
        "pk": [
            "currency_rk",
            "data_actual_date",
        ],
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
        "date_columns": [
            "data_actual_date",
            "data_actual_end_date",
        ],
        "numeric_columns": [
            "currency_rk",
            "reduced_cource",
        ],
        "pk": [
            "data_actual_date",
            "currency_rk",
        ],
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
        "date_columns": [
            "start_date",
            "end_date",
        ],
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
        "pk": [
            "ledger_account",
            "start_date",
        ],
        "truncate_before_load": False,
    },
}


# 4. Функции преобразования значений
def normalize_column_name(column_name: str) -> str:
    return (
        str(column_name)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("\ufeff", "")
    )


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

#Приводит числовое значение к float.
def parse_number(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "" or value.lower() in ("nan", "none", "null"):
        return None

    value = value.replace(" ", "")
    value = value.replace(",", ".")

    return float(value)


def clean_text(value):
    """
    Приводит текстовые значения к нормальному виду.
    Пустые строки заменяет на None.
    """

    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "" or value.lower() in ("nan", "none", "null"):
        return None

    return value


# 5. Функции логирования
def log_start(conn, process_name, target_table, source_file):
    """
    Записывает старт загрузки в logs.etl_load_log.
    Возвращает log_id.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO logs.etl_load_log (
                process_name,
                target_table,
                source_file,
                status,
                started_at
            )
            VALUES (%s, %s, %s, 'STARTED', now())
            RETURNING log_id;
            """,
            (
                process_name,
                target_table,
                str(source_file),
            ),
        )

        log_id = cur.fetchone()[0]

    conn.commit()
    return log_id


def log_success(
    conn,
    log_id,
    rows_in_file,
    rows_loaded,
    rows_error,
    extra_info=None,
):
    """
    Обновляет лог в случае успешной загрузки.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE logs.etl_load_log
            SET status = 'SUCCESS',
                finished_at = now(),
                rows_in_file = %s,
                rows_loaded = %s,
                rows_error = %s,
                error_message = NULL,
                extra_info = %s
            WHERE log_id = %s;
            """,
            (
                rows_in_file,
                rows_loaded,
                rows_error,
                json.dumps(extra_info or {}, ensure_ascii=False),
                log_id,
            ),
        )

    conn.commit()


def log_failed(
    conn,
    log_id,
    error_message,
    rows_in_file=None,
    rows_loaded=None,
    rows_error=None,
):
    """
    Обновляет лог в случае ошибки.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE logs.etl_load_log
            SET status = 'FAILED',
                finished_at = now(),
                rows_in_file = %s,
                rows_loaded = %s,
                rows_error = %s,
                error_message = %s
            WHERE log_id = %s;
            """,
            (
                rows_in_file,
                rows_loaded,
                rows_error,
                str(error_message),
                log_id,
            ),
        )

    conn.commit()



# 6. Чтение CSV
def read_csv_file(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    encodings = [
        "utf-8-sig",
        "utf-8",
        "cp1251",
        "windows-1251",
        "latin1",
    ]

    separators = [
        ";",
        ",",
    ]

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

                if len(df.columns) == 1:
                    last_error = (
                        f"Файл {file_path.name} прочитан как одна колонка "
                    )
                    continue

                df.columns = [
                    normalize_column_name(col)
                    for col in df.columns
                ]

                print(
                    f"[INFO] Файл {file_path.name} прочитан: "
                    f"encoding={encoding}, sep='{separator}'"
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



# 7. Трансформация DataFrame
def transform_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:   #приводит dataframe к структуре целевой таблицы
    required_columns = config["columns"]
    pk_columns = config.get("pk") or []

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    missing_pk_columns = [
        col for col in pk_columns       #поиск отсутствующих колонок 
        if col not in df.columns
    ]

    if missing_pk_columns:
        raise ValueError(
            f"В CSV отсутствуют ключевые колонки: {missing_pk_columns}. "
            f"Фактические колонки файла: {list(df.columns)}"
        )

    if missing_columns:
        print(
            f"[WARNING] В CSV отсутствуют колонки: {missing_columns}. "
            f"Они будут добавлены со значением NULL."
        )

        for column in missing_columns:
            df[column] = None

    df = df[required_columns].copy()

    for column in config["date_columns"]:      #Преобразование дат 
        df[column] = df[column].apply(parse_date)

    for column in config["numeric_columns"]:    #Преобразование чисел 
        df[column] = df[column].apply(parse_number)

    text_columns = [                            #Определение текстовых колонок
        col for col in config["columns"]
        if col not in config["date_columns"]
        and col not in config["numeric_columns"]
    ]

    for column in text_columns:
        df[column] = df[column].apply(clean_text)   #Очитска текстовых колонок 

    df = df.where(pd.notnull(df), None) #замена Nan на None

    if pk_columns:
        duplicates_count = df.duplicated(
            subset=pk_columns,
            keep=False
        ).sum()

        if duplicates_count > 0:
            print(
                f"[WARNING] Найдены дубли по ключу {pk_columns}: "
                f"{duplicates_count} строк. "
                f"Оставляем последнюю запись по каждому ключу."
            )

            df = df.drop_duplicates(
                subset=pk_columns,
                keep="last"  #оставляем последнюю строку
            )

    return df


# 8. SQL-генераторы
def build_insert_sql(target_table: str, columns: list[str]) -> str:
    """
    Строит обычный INSERT.
    Используется для таблиц без первичного ключа.
    """

    columns_sql = ", ".join(columns)   #Сбор списка колонок 

    sql = f"""
        INSERT INTO {target_table} ({columns_sql})
        VALUES %s;
    """

    return sql


def build_upsert_sql(target_table: str, columns: list[str], pk_columns: list[str]) -> str:
    """
    Используется для таблиц с первичным ключом.
    """

    columns_sql = ", ".join(columns)
    conflict_sql = ", ".join(pk_columns)

    update_columns = [
        col for col in columns
        if col not in pk_columns  #Исключение ключевых полей из списк
    ]

    update_sql = ", ".join(
        [
            f"{col} = EXCLUDED.{col}"
            for col in update_columns
        ]
    )

    sql = f"""
        INSERT INTO {target_table} ({columns_sql})
        VALUES %s
        ON CONFLICT ({conflict_sql})
        DO UPDATE SET {update_sql};
    """

    return sql


# 9. Загрузка одной таблицы
def load_table(conn, table_name: str, config: dict):
    """
    Полный процесс загрузки одной таблицы:
    - лог STARTED;
    - пауза 5 секунд;
    - чтение CSV;
    - трансформация;
    - TRUNCATE или UPSERT;
    - лог SUCCESS/FAILED.
    """

    process_name = "load_csv_to_ds"
    target_table = config["target"]
    source_file = config["file"]

    rows_in_file = 0
    rows_loaded = 0
    rows_error = 0

    log_id = log_start(
        conn=conn,
        process_name=process_name,
        target_table=target_table,
        source_file=source_file,
    )

    print(f"[STARTED] {target_table} из файла {source_file}")

    # Пауза
    time.sleep(5)

    try:
        df_raw = read_csv_file(source_file)
        rows_in_file = len(df_raw)

        df = transform_dataframe(df_raw, config)

        rows = [
            tuple(row)
            for row in df.to_numpy()
        ]

        with conn.cursor() as cur:
            if config["truncate_before_load"]:
                print(f"[TRUNCATE] {target_table}")
                cur.execute(f"TRUNCATE TABLE {target_table};")

            if not rows:
                print(f"[WARNING] Файл пустой: {source_file}")
            else:
                if config["pk"] is None:
                    sql = build_insert_sql(
                        target_table=target_table,
                        columns=config["columns"],
                    )
                else:
                    sql = build_upsert_sql(
                        target_table=target_table,
                        columns=config["columns"],
                        pk_columns=config["pk"],
                    )

                execute_values(         #Массовая загрузка 
                    cur,
                    sql,
                    rows,
                    page_size=1000,
                )

        conn.commit()

        rows_loaded = len(rows)
        rows_error = rows_in_file - rows_loaded

        extra_info = {
            "table_name": table_name,
            "target_table": target_table,
            "source_file": str(source_file),
            "truncate_before_load": config["truncate_before_load"],
            "primary_key": config["pk"],
        }

        log_success(
            conn=conn,
            log_id=log_id,
            rows_in_file=rows_in_file,
            rows_loaded=rows_loaded,
            rows_error=rows_error,
            extra_info=extra_info,
        )

        print(f"[SUCCESS] {target_table}: загружено строк {rows_loaded}")

    except Exception as error:
        conn.rollback()

        rows_error = rows_in_file - rows_loaded if rows_in_file else None

        log_failed(
            conn=conn,
            log_id=log_id,
            error_message=error,
            rows_in_file=rows_in_file,
            rows_loaded=rows_loaded,
            rows_error=rows_error,
        )

        print(f"[FAILED] {target_table}: {error}")

        raise





def main():

    print("[INFO] Старт ETL загрузки CSV в слой DS")
    print(f"[INFO] PROJECT_DIR = {PROJECT_DIR}")
    print(f"[INFO] DATA_DIR = {DATA_DIR}")

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        for table_name, config in TABLE_CONFIG.items():         #проходит по всем таблицам и загружает 
            load_table(
                conn=conn,
                table_name=table_name,
                config=config,
            )

        print("[INFO] ETL успешно завершён")

    finally:
        conn.close()
        print("[INFO] Соединение с БД закрыто")


if __name__ == "__main__":
    main()