import psycopg2
from psycopg2.extensions import connection, cursor
from configparser import ConfigParser

from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur: cursor, conn: connection) -> None:
    """Drops all of the tables in the database."""
    for query in drop_table_queries:
        cur.execute(query)
    conn.commit()


def create_tables(cur: cursor, conn: connection) -> None:
    """Creates the the tables in the database."""
    for query in create_table_queries:
        cur.execute(query)
    conn.commit()


def main():
    """Drops the existing tables and then recreates them in Redshift."""
    config: ConfigParser = ConfigParser()
    config.read("dwh.cfg")

    conn: connection = psycopg2.connect(
        "host={host} dbname={dbname} user={user} password={password} port={port}".format(
            **dict(config["CLUSTER"].items())
        )
    )
    cur: cursor = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
