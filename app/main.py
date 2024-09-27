import os
import sys
import time
from pathlib import Path
from typing import Callable
import psycopg2
from psycopg2.extensions import connection as PgConnection
from elasticsearch import Elasticsearch
from elastic import create_index, index_data, wait_for_elasticsearch
from match import match_and_update_similar_sku
from db import create_table, parse_and_insert_categories, parse_xml_and_insert_to_db


# # Получение переменной окружения для подключения к базе данных
# DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://admin:password@postgres:5432/marketplace")


def get_postgres_connection() -> PgConnection:
    """Подключение к базе данных PostgresSQL."""
    return psycopg2.connect(
        host="postgres",
        database="marketplace",
        user="admin",
        password="password"
    )


def get_elastic_connection() -> Elasticsearch:
    """Подключение к Elasticsearch."""
    return Elasticsearch(
        "http://elasticsearch:9200",
        basic_auth=('elastic', 'your_password')
    )


def wait_for_postgres(get_connection_func: Callable[[], PgConnection], timeout: int = 30, interval: int = 2) -> bool:
    """
    Ожидаем, пока PostgresSQL станет доступным, используя функцию подключения.
    :param get_connection_func: Функция, возвращающая подключение к PostgresSQL.
    :param timeout: Максимальное время ожидания в секундах.
    :param interval: Интервал между попытками в секундах.
    :return: True, если PostgresSQL стал доступен, иначе False.
    """
    start_time = time.time()
    while True:
        try:
            connection = get_connection_func()
            connection.close()
            print("PostgresSQL доступен!")
            return True
        except Exception:
            pass  # Подключение к базе данных пока недоступно, ждем

        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print("Ошибка: превышен тайм-аут ожидания PostgresSQL.")
            return False

        print("Ожидание PostgresSQL...")
        time.sleep(interval)


if __name__ == '__main__':
    # Ожидаем, пока PostgresSQL станет доступен
    if not wait_for_postgres(get_postgres_connection):
        print("Не удалось подключиться к PostgresSQL, выход.")
        exit(1)

    # Создаем таблицы в базе данных
    create_table()

    # Имя XML файла для обработки
    input_file: str = 'test.xml'

    # Парсинг категорий и вставка данных в базу данных
    parse_and_insert_categories(input_file)
    parse_xml_and_insert_to_db(input_file)

    # Подключаемся к PostgresSQL и Elasticsearch
    conn: PgConnection = get_postgres_connection()
    es: Elasticsearch = get_elastic_connection()

    # Ожидаем, пока Elasticsearch станет доступен
    wait_for_elasticsearch(es)

    # Удаляем индекс "products", если он существует, и создаем новый
    es.options(ignore_status=[400, 404]).indices.delete(index="products")
    create_index(es)

    # Индексация данных из PostgresSQL в Elasticsearch
    index_data(es, conn)

    # Поиск и обновление похожих товаров (SKU) в базе данных
    match_and_update_similar_sku(es, conn)

    # Закрываем подключение к PostgresSQL
    conn.close()
