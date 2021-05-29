from typing import List
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        select_from_multiple_tables: List[str],
        insert_into_table: str,
        select_query: str,
        redshift_conn_id: str = "redshift_conn_id",
        *args,
        **kwargs,
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_query = f"INSERT INTO {insert_into_table} " + select_query.format(
            *select_from_multiple_tables
        )
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Running LoadFactOperator")
        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run(self.sql_query)
