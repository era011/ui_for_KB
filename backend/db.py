import os
from psycopg2 import pool
from psycopg2.extras import DictCursor
from contextlib import contextmanager
from langchain_core.documents import Document
from const import *

pg_pool = None

def init_pg_pool():
    global pg_pool
    if pg_pool is None:
        pg_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dbname=os.getenv("POSTGRES_DB", "operators"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            host=os.getenv("SERVER", "postgres"),
            port=os.getenv("POSTGRES_PORT", 5433),
            cursor_factory=DictCursor
        )
        print("Пул соединений с Postgres инициализирован")
    return pg_pool

def ensure_marketing_files_table():
    with get_pg_cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_TABLE_NAME} (
                id_doc TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                added_date TIMESTAMP NOT NULL,
                chunks_count INTEGER NOT NULL DEFAULT 0
            );
        """)

def get_connection():
    if pg_pool is None:
        raise RuntimeError("Пул соединений не инициализирован. Сначала вызови init_pg_pool().")
    return pg_pool.getconn()

def release_connection(conn):
    if pg_pool is not None:
        pg_pool.putconn(conn)



@contextmanager
def get_pg_cursor():
    conn = None
    cur = None
    try:
        conn = get_connection() 
        cur = conn.cursor()
        yield cur               
        conn.commit()                 
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            release_connection(conn)


def delete_document_postgres(id_doc: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {PG_TABLE_NAME} WHERE id_doc = %s;", (id_doc,))
        conn.commit()
        cur.close()
        print(f"🗑 Документ {id_doc} удалён из Postgres")
    except Exception as e:
        conn.rollback()
        raise ValueError(f"Ошибка при удалении из Postgres: {e}")
    finally:
        release_connection(conn)


# def add_document_to_postgres(doc: Document):
#     conn = None
#     try:
#         conn = get_connection()
#         cur = conn.cursor()
#         cur.execute(f"""
#             INSERT INTO {PG_TABLE_NAME} (id_doc, name, type_doc, organization, source, added_date)
#             VALUES (%s, %s, %s, %s, %s, %s)
#             ON CONFLICT (id_doc) DO NOTHING;
#         """, (
#             doc.metadata.get("id_doc"),
#             doc.metadata.get("name"),
#             doc.metadata.get("added_date_to_weaviate")
#         ))
#         conn.commit()
#         cur.close()
#         print(f"✅ Документ {doc.metadata.get('name')} добавлен в Postgres")
#     except Exception as e:
#         raise ValueError(f"Ошибка при добавлении документа в Postgres: {e}")
#     finally:
#         if conn:
#             release_connection(conn)
            
def add_document_to_postgres(doc: Document, chunks_count:int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO {PG_TABLE_NAME} (id_doc, name, added_date, chunks_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_doc) DO NOTHING;
        """, (
            doc.metadata.get("id_doc"),
            doc.metadata.get("name"),
            doc.metadata.get("added_date_to_weaviate"),
            chunks_count
        ))
        conn.commit()
        cur.close()
        print(f"✅ Документ {doc.metadata.get('name')} добавлен в Postgres")
    except Exception as e:
        if conn:
            conn.rollback()
        raise ValueError(f"Ошибка при добавлении документа в Postgres: {e}")
    finally:
        if conn:
            release_connection(conn)


def search_marketing_files_pg(title=None):
    try:
        query = f"""
            SELECT *
            FROM {PG_TABLE_NAME}
        """
        conditions = []
        params = []

        if title:
            conditions.append("name ILIKE %s")
            params.append(f"%{title}%")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY added_date DESC"


        with get_pg_cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        results = [
            {
                "id_doc": row[0],
                "name": row[1],
                "added_date": row[2],
                "chunks_count": row[3]
            }
            for row in rows
        ]

        return results

    except Exception as e:
        print(f"Ошибка при поиске документов в Postgres: {e}")
        return []