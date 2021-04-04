from typing import List

import psycopg2
from psycopg2.extensions import connection, cursor
from configparser import ConfigParser
import logging

from sql_queries import load_staging_table_queries, insert_table_queries

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger: logging.Logger = logging.getLogger(__name__)


def run_queries(cur: cursor, queries: List[str]) -> None:
    """Executes a list of SQL queries, using the given cursor."""
    for query in queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)


def main() -> None:
    """Run the ETL pipeline to copy the data into Redshift."""
    config: ConfigParser = ConfigParser()
    config.read("dwh.cfg")

    conn: connection = psycopg2.connect(
        "host={host} dbname={dbname} user={user} password={password} port={port}".format(
            **dict(config["CLUSTER"].items())
        )
    )
    cur: cursor = conn.cursor()

    run_queries(cur, load_staging_table_queries)
    run_queries(cur, insert_table_queries)

    logger.info("Commiting to the database...")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
