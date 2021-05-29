from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Airflow Operator for Loading dimension tables to Refshift.
    
    It can be parametrized and optionally truncate the table before inserting the data.
    """
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        select_from_table: str,
        insert_into_table: str,
        select_query: str,
        redshift_conn_id: str = "redshift_conn_id",
        empty_before_load=True,
        *args,
        **kwargs,
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql_query = f"INSERT INTO {insert_into_table}\n" + select_query.format(
            select_from_table=select_from_table
        )
        if empty_before_load:
            self.sql_query = f"TRUNCATE {insert_into_table} ;\n" + self.sql_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Running LoadDimensionOperator")
        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run(self.sql_query)
