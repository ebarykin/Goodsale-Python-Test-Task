from elasticsearch import Elasticsearch
from psycopg2.extensions import connection as PgConnection


def search_similar_products(es: Elasticsearch, product: dict) -> list:
    """
    Ищет похожие товары в Elasticsearch на основе заданного продукта.

    :param es: Экземпляр Elasticsearch для выполнения запросов.
    :param product: Словарь, содержащий данные продукта (uuid, title, description, brand).
    :return: Список похожих товаров.
    """
    search_body = {
        "query": {
            "more_like_this": {
                "fields": ["title", "description", "brand", "features"],
                "like": [
                    {
                        "_index": "products",
                        "_id": product['uuid']
                    }
                ],
                "min_term_freq": 2,
                "min_doc_freq": 2,
                "max_query_terms": 10,
            }
        },
        "size": 10,
        "sort": [
            {"_score": {"order": "desc"}}
        ]
    }

    response = es.search(index="products", body=search_body)

    # Фильтрация результатов по схожести
    threshold = 0.5
    filtered_items = []

    for hit in response["hits"]["hits"]:
        similarity_score = hit["_score"]  # Получаем оценку схожести
        if similarity_score >= threshold:
            filtered_items.append(hit)

    # Оставляем только два самых похожих товара
    result = sorted(filtered_items, key=lambda x: x["_score"], reverse=True)[:2]

    return result


def update_similar_sku(conn: PgConnection, product_uuid: str, similar_uuids: list) -> None:
    """
    Обновляет поле similar_sku в таблице PostgresSQL для заданного товара.

    :param conn: Соединение с базой данных PostgresSQL.
    :param product_uuid: UUID товара, для которого обновляется поле similar_sku.
    :param similar_uuids: Список UUID похожих товаров.
    """
    cursor = conn.cursor()
    similar_uuids_str = [str(u) for u in similar_uuids]

    cursor.execute(
        """
        UPDATE public.sku
        SET similar_sku = %s::uuid[]
        WHERE uuid = %s
        """,
        (similar_uuids_str, product_uuid)
    )
    conn.commit()


def match_and_update_similar_sku(es: Elasticsearch, conn: PgConnection) -> None:
    """
    Ищет и обновляет похожие товары для всех товаров в базе данных PostgreSQL.

    :param es: Экземпляр Elasticsearch для выполнения запросов.
    :param conn: Соединение с базой данных PostgreSQL.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT uuid, title, description, brand FROM public.sku")

    for row in cursor.fetchall():
        product = {
            "uuid": row[0],
            "title": row[1],
            "description": row[2],
            "brand": row[3],
        }

        # Поиск похожих товаров
        similar_products = search_similar_products(es, product)
        # if similar_products:
        #     print(f"Товар {product['title']}{product['uuid']} похож на:")
        #     for similar in similar_products:
        #         print(f"- {similar['_source']['uuid']}: {similar['_source']['title']}")

        # Получение UUID похожих товаров
        similar_uuids = [similar['_source']['uuid'] for similar in similar_products]

        # Обновление поля similar_sku
        update_similar_sku(conn, product["uuid"], similar_uuids)
