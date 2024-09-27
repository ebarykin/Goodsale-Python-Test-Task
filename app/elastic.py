from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import time
from psycopg2.extensions import connection as _connection, cursor as _cursor
from typing import List, Dict, Any


def wait_for_elasticsearch(es: Elasticsearch, timeout: int = 30, interval: int = 2) -> bool:
    """
    Ожидаем, пока Elasticsearch станет доступным.
    :param es: Экземпляр Elasticsearch.
    :param timeout: Максимальное время ожидания в секундах.
    :param interval: Интервал между попытками в секундах.
    :return: True, если Elasticsearch доступен, False, если превышен тайм-аут.
    """
    start_time = time.time()
    while True:
        try:
            if es.ping():
                print("Elasticsearch доступен!")
                return True
        except Exception as e:
            print(f"Ошибка подключения к Elasticsearch: {e}")

        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print("Ошибка: превышен тайм-аут ожидания Elasticsearch.")
            return False

        print("Ожидание Elasticsearch...")
        time.sleep(interval)


def create_index(es: Elasticsearch) -> None:
    """
    Создает индекс в Elasticsearch, если его еще нет.
    :param es: Экземпляр Elasticsearch.
    """
    index_body = {
        "mappings": {
            "properties": {
                "uuid": {"type": "keyword"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "brand": {"type": "keyword"},
                "features": {"type": "object"},
            }
        }
    }
    es.indices.create(index="products", body=index_body, ignore=400)


def index_data(es: Elasticsearch, conn: _connection) -> None:
    """
    Загружает товары из PostgreSQL и индексирует их в Elasticsearch.
    :param es: Экземпляр Elasticsearch.
    :param conn: Соединение с PostgreSQL.
    """
    cursor: _cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM public.sku")
    count_records: int = cursor.fetchone()[0]
    print(f"Количество записей в PostgreSQL: {count_records}")

    cursor.execute("SELECT uuid, marketplace_id, product_id, title, description, brand, features FROM public.sku")

    actions: List[Dict[str, Any]] = []
    for row in cursor.fetchall():
        doc: Dict[str, Any] = {
            "_index": "products",
            "_id": row[0],  # uuid
            "_source": {
                "uuid": row[0],
                "marketplace_id": row[1],
                "product_id": row[2],
                "title": row[3],
                "description": row[4],
                "brand": row[5],
                "features": row[6],
            }
        }
        actions.append(doc)

    bulk(es, actions)
    print(f"Успешно проиндексировано {len(actions)} документов.")
