import argparse
import logging
import os
import time
import uuid
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


#пути
PROJECT_DIR = Path(__file__).resolve().parents[1]
EXPORT_DIR = PROJECT_DIR / "exports"
ENV_FILE = PROJECT_DIR / ".env"
EXPORT_DIR.mkdir(exist_ok=True)



# 2.логирование в консоли
logging.basicConfig(
    level=logging.INFO,     #вывод информационного сообщения
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)            #создание логера, через который дальше выводятся сообщения 

def log_partition(title: str) -> None:          #функция отделения этапав в логах
    logger.info("-" * 70)
    logger.info(title)
    logger.info("-" * 70)

#подключение к бд
load_dotenv(ENV_FILE)

DB_CONFIG = {                                   #сл параметры подключения
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


SOURCE_TABLE = "dm.dm_f101_round_f"
TARGET_TABLE = "dm.dm_f101_round_f_v2"
LOG_TABLE = "logs.f101_csv_exchange_log"
DEFAULT_EXPORT_FILE = EXPORT_DIR / "dm_f101_round_f_january_2018.csv"
FROM_DATE = "2018-01-01"
TO_DATE = "2018-01-31"


#функция создания логовой таблицы 
def create_log_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS logs;

            CREATE TABLE IF NOT EXISTS logs.f101_csv_exchange_log (
                log_id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL,
                operation_type TEXT NOT NULL,
                source_table TEXT,
                target_table TEXT,
                file_path TEXT,
                status TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                duration_seconds NUMERIC(12, 2),
                rows_processed INTEGER,
                error_message TEXT
            );
            """
        )
    conn.commit()
    logger.info("Логовая таблица logs.f101_csv_exchange_log существует")


def log_start(          #созданиен записи о начале операции 
    conn,
    run_id: uuid.UUID,
    operation_type: str,
    source_table: str | None,
    target_table: str | None,
    file_path: Path,
) -> int:
    with conn.cursor() as cur:
        cur.execute(                                                       # возвращает id созданной записи, используется для обновления записи в логах
            """
            INSERT INTO logs.f101_csv_exchange_log (
                run_id,
                operation_type,
                source_table,
                target_table,
                file_path,
                status,
                started_at
            )
            VALUES (%s, %s, %s, %s, %s, 'STARTED', now())           
            RETURNING log_id;                                       
            """,
            (
                str(run_id),
                operation_type,
                source_table,
                target_table,
                str(file_path),
            ),
        )
        log_id = cur.fetchone()[0]

    conn.commit()
    return log_id


def log_success(            #функция обновления записи в случае успешного выполнения
    conn,
    log_id: int,
    rows_processed: int,
    duration_seconds: float,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE logs.f101_csv_exchange_log
            SET status = 'SUCCESS',
                finished_at = now(),
                duration_seconds = %s,
                rows_processed = %s,
                error_message = NULL
            WHERE log_id = %s;
            """,
            (
                duration_seconds,
                rows_processed,
                log_id,
            ),
        )
    conn.commit()


def log_failed(
    conn,
    log_id: int,
    error_message: str,
    rows_processed: int | None,
    duration_seconds: float,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE logs.f101_csv_exchange_log
            SET status = 'FAILED',
                finished_at = now(),
                duration_seconds = %s,
                rows_processed = %s,
                error_message = %s
            WHERE log_id = %s;
            """,
            (
                duration_seconds,
                rows_processed,
                error_message,
                log_id,
            ),
        )
    conn.commit()


#функция получения списка колонок таблицы из системного представления
def get_table_columns(conn, schema_name: str, table_name: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (schema_name, table_name),
        )
        columns = [row[0] for row in cur.fetchall()]
    return columns



#созждает копию таблицы 101 формы
def ensure_target_table(conn) -> None:
    """
    Создаёт копию таблицы dm.dm_f101_round_f.
    Если таблица уже существует, не пересоздаёт её.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
            (LIKE {SOURCE_TABLE} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
            """
        )                                                                                       #создает таблицу с такой же 
    conn.commit()
    logger.info(f"Таблица {TARGET_TABLE} существует")


#выгрузка в csv
def export_f101_to_csv(conn, output_file: Path) -> int:
    """
    Выгружает данные за январь 2018 из dm.dm_f101_round_f в CSV.
    """
    log_partition("выгрузка 101 формы в csv")
    output_file.parent.mkdir(exist_ok=True)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {SOURCE_TABLE}
            WHERE from_date = DATE %s AND to_date = DATE %s;
            """,
            (FROM_DATE, TO_DATE),
        )
        rows_count = cur.fetchone()[0]

    copy_sql = f"""                 
        COPY (
            SELECT
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
            FROM {SOURCE_TABLE}
            WHERE from_date = DATE '{FROM_DATE}' AND to_date = DATE '{TO_DATE}'
            ORDER BY chapter, ledger_account, characteristic
        )
        TO STDOUT
        WITH CSV HEADER DELIMITER ';' NULL '' ENCODING 'UTF8';      
    """
#heder - первая строка содержит название колонок; NULL выгружаются как пустые значения 

    with conn.cursor() as cur:              #открытие файла на запись 
        with open(output_file, "w", encoding="utf-8-sig", newline="") as file:
            cur.copy_expert(copy_sql, file)
    logger.info(f"CSV-файл создан: {output_file}")
    logger.info(f"Выгружено строк: {rows_count}")
    return rows_count #значение записывается в лог



#импорт csv в копию таблицы 
def import_f101_from_csv(conn, input_file: Path) -> int:
    """
    Загружает CSV-файл в копию таблицы 101 формы: dm.dm_f101_round_f_v2.
    Перед загрузкой таблица очищается.
    """
    log_partition("импорт csv в dm.dm_f101_round_f_v2")

    ensure_target_table(conn)   #вызов функции проверки\создания копии таблицы

    columns = get_table_columns(conn, "dm", "dm_f101_round_f") #получает список колонок исходной таблицы
    columns_sql = ", ".join(columns)

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {TARGET_TABLE};")  #для исключения дубликатов при повторном импорте

        copy_sql = f"""
            COPY {TARGET_TABLE} ({columns_sql})
            FROM STDIN      
            WITH CSV HEADER DELIMITER ';' NULL '' ENCODING 'UTF8';
        """
        #STDIN данные подаются из файла через python

        with open(input_file, "r", encoding="utf-8-sig", newline="") as file:
            cur.copy_expert(copy_sql, file)     #загрузка содержимого в таблицу

        cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE};")
        rows_count = cur.fetchone()[0]          #подсчет количества строк в таблице v2
    conn.commit()

    logger.info(f"CSV-файл импортирован: {input_file}")
    logger.info(f"Загружено строк в {TARGET_TABLE}: {rows_count}")
    return rows_count


#логика процесса
def run_export(output_file: Path) -> None:  #управляющая функция для режима экспорт
    run_id = uuid.uuid4()  #уникальный идентификатор записи
    started_at = time.time()

    conn = get_connection()
    try:
        create_log_table(conn)

        log_id = log_start(
            conn=conn,
            run_id=run_id,
            operation_type="EXPORT_F101_TO_CSV",
            source_table=SOURCE_TABLE,
            target_table=None,
            file_path=output_file,
        )
        rows_count = export_f101_to_csv(conn, output_file)
        duration = round(time.time() - started_at, 2)

        log_success(
            conn=conn,
            log_id=log_id,
            rows_processed=rows_count,
            duration_seconds=duration,
        )
        logger.info("выгрузка успешно завершена")

    except Exception as error:
        conn.rollback()
        duration = round(time.time() - started_at, 2)

        logger.exception(f"ошибка выгрузки csv: {error}")
        raise

    finally:
        conn.close()
        logger.info("соединение с бд закрыто")


def run_import(input_file: Path) -> None:
    run_id = uuid.uuid4()
    started_at = time.time()
    conn = get_connection()

    try:
        create_log_table(conn)

        log_id = log_start(
            conn=conn,
            run_id=run_id,
            operation_type="IMPORT_F101_FROM_CSV",
            source_table=None,
            target_table=TARGET_TABLE,
            file_path=input_file,
        )

        rows_count = import_f101_from_csv(conn, input_file)

        duration = round(time.time() - started_at, 2) #подсчет длительности процесса

        log_success(
            conn=conn,
            log_id=log_id,
            rows_processed=rows_count,
            duration_seconds=duration,
        )
        logger.info("импорт успешно завершён")
    except Exception as error:
        conn.rollback()
        duration = round(time.time() - started_at, 2)
        logger.exception(f"ошибка импорта CSV: {error}")
        raise
    finally:
        conn.close()
        logger.info("соединение с БД закрыто")


def main() -> None:
    parser = argparse.ArgumentParser(                   #создается объект принимающий аргументы командной строки 
        description="выгрузка и импорт 101 формы в CSV"
    )

    parser.add_argument(
        "mode",
        choices=["export", "import"],
        help="режим работы: export или import",
    )

    parser.add_argument(
        "--file",
        default=str(DEFAULT_EXPORT_FILE),
        help="путь к CSV-файлу",
    )

    args = parser.parse_args()
    file_path = Path(args.file)    #читает агрументы командной строки и превращает путь к файлу в объект 

    if args.mode == "export":       #выбор режима
        run_export(file_path)
    elif args.mode == "import":
        run_import(file_path)


if __name__ == "__main__":
    main()