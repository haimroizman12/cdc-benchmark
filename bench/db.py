from __future__ import annotations
import os
import psycopg2
import pymssql


def pg_connect():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "postgres"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def mssql_connect():
    return pymssql.connect(
        server=os.environ.get("MSSQL_HOST", "mssql"),
        port=int(os.environ.get("MSSQL_PORT_INTERNAL", "1433")),
        user="sa",
        password=os.environ["MSSQL_SA_PASSWORD"],
        database=os.environ.get("MSSQL_DB", "target_db"),
    )
