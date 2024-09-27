import os
from lxml import etree
from psycopg2 import connect, sql
import json
import uuid
from typing import Dict, List
from pathlib import Path

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password@postgres:5432/marketplace")


def parse_and_insert_categories(file_path: str) -> None:
    """
    Парсинг категории из XML-файла и вставка в таблицу category.
    """
    conn = connect(DATABASE_URL)
    cursor = conn.cursor()

    context = etree.iterparse(file_path, events=('end',), tag='category')
    for event, elem in context:
        category_id = int(elem.get('id'))
        parent_id = elem.get('parentId')
        name = elem.text

        cursor.execute(
            "INSERT INTO public.category (id, parent_id, name) VALUES (%s, %s, %s)",
            (category_id, parent_id, name)
        )
        conn.commit()
        elem.clear()

    cursor.close()
    conn.close()


def parse_xml_and_insert_to_db(file_path: str) -> None:
    """
    Парсинг товаров из XML-файла и заполнение таблицы sku.
    """
    conn = connect(DATABASE_URL)
    cursor = conn.cursor()

    context = etree.iterparse(file_path, events=('end',), tag='offer')

    for event, elem in context:
        category_id = int(elem.findtext('categoryId'))

        cursor.execute("""
            WITH RECURSIVE category_path AS (
                SELECT id, parent_id, name, 1 AS level
                FROM public.category
                WHERE id = %s
                UNION ALL
                SELECT c.id, c.parent_id, c.name, cp.level + 1
                FROM public.category c
                JOIN category_path cp ON cp.parent_id = c.id
            )
            SELECT array_agg(name ORDER BY level DESC) AS full_path
            FROM category_path;
        """, (category_id,))

        full_path: List[str] = cursor.fetchone()[0] or []

        category_lvl_1 = full_path[0] if len(full_path) > 0 else None
        category_lvl_2 = full_path[1] if len(full_path) > 1 else None
        category_lvl_3 = full_path[2] if len(full_path) > 2 else None
        category_remaining = '/'.join(full_path[3:]) if len(full_path) > 3 else None

        features: Dict[str, str] = {}
        for param in elem.findall('param'):
            param_name = param.get('name')
            param_value = param.text
            if param_name and param_value:
                features[param_name] = param_value

        json_features = json.dumps(features, ensure_ascii=False)

        data = {
            'uuid': str(uuid.uuid4()),
            'marketplace_id': 1,
            'product_id': elem.get('id'),
            'title': elem.findtext('name'),
            'description': elem.findtext('description'),
            'brand': elem.findtext('vendor'),
            'seller_id': None,
            'seller_name': elem.findtext('vendorCode'),
            'first_image_url': elem.findtext('picture'),
            'category_id': int(elem.findtext('categoryId')),
            'category_lvl_1': category_lvl_1,
            'category_lvl_2': category_lvl_2,
            'category_lvl_3': category_lvl_3,
            'category_remaining': category_remaining,
            'features': json_features,
            'rating_count': 0,
            'rating_value': 0.0,
            'price_before_discounts': float(elem.findtext('price', '0.0')),
            'discount': 0.0,
            'price_after_discounts': float(elem.findtext('price', '0.0')),
            'bonuses': 0,
            'sales': 0,
            'currency': elem.findtext('currencyId'),
            'barcode': elem.findtext('barcode')
        }

        insert_query = sql.SQL("""
            INSERT INTO public.sku
            (uuid, marketplace_id, product_id, title, description, brand, seller_id, seller_name, first_image_url,
            category_id, category_lvl_1, category_lvl_2, category_lvl_3, category_remaining, features, rating_count,
            rating_value, price_before_discounts, discount, price_after_discounts, bonuses, sales, currency, barcode)
            VALUES
            (%(uuid)s, %(marketplace_id)s, %(product_id)s, %(title)s, %(description)s, %(brand)s, %(seller_id)s,
            %(seller_name)s, %(first_image_url)s, %(category_id)s, %(category_lvl_1)s, %(category_lvl_2)s,
            %(category_lvl_3)s, %(category_remaining)s, %(features)s, %(rating_count)s, %(rating_value)s,
            %(price_before_discounts)s, %(discount)s, %(price_after_discounts)s, %(bonuses)s, %(sales)s,
            %(currency)s, %(barcode)s)
        """)

        cursor.execute(insert_query, data)
        conn.commit()
        elem.clear()

    cursor.close()
    conn.close()


def create_table() -> None:
    """
    Создание таблиц sku и category.
    """
    conn = connect(DATABASE_URL)
    # conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    create_sku_table_query = '''
    CREATE TABLE IF NOT EXISTS public.sku (
        uuid uuid PRIMARY KEY,
        marketplace_id integer,
        product_id bigint,
        title text,
        description text,
        brand text,
        seller_id integer,
        seller_name text,
        first_image_url text,
        category_id integer,
        category_lvl_1 text,
        category_lvl_2 text,
        category_lvl_3 text,
        category_remaining text,
        features json,
        rating_count integer,
        rating_value double precision,
        price_before_discounts real,
        discount double precision,
        price_after_discounts real,
        bonuses integer,
        sales integer,
        inserted_at timestamp default now(),
        updated_at timestamp default now(),
        currency text,
        barcode text,
        similar_sku uuid[]
    );
    '''
    cursor.execute(create_sku_table_query)

    create_category_table_query = '''
    CREATE TABLE IF NOT EXISTS public.category (
        id integer PRIMARY KEY,
        parent_id integer,
        name text
    );
    '''
    cursor.execute(create_category_table_query)

    print("Таблицы созданы успешно.")
    conn.commit()
    cursor.close()
    conn.close()
