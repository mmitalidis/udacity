from typing import Any, List, Tuple

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Data quality checks for Redshift tables."""

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        sql_queries: List[str],
        expected_results: List[Tuple[Any]],
        redshift_conn_id: str = "redshift_conn_id",
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries
        self.expected_results = expected_results
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        for query, expected_result in zip(self.sql_queries, self.expected_results):
            result = redshift.get_first(query)
            if result != expected_result:
                raise ValueError(
                    f"Data quality check failed. Expected: {expected_result}. Got: {result}"
                )
