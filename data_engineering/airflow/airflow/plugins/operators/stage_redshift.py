from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Airflow Operator for copying JSON files from S3 to Redshift."""
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        table: str,
        s3_object: str,
        iam_role: str,
        json_path: str = "auto",
        region: str = "us-west-2",
        redshift_conn_id: str = "redshift_conn_id",
        template_s3_object: bool = True,
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.sql_query = """
            COPY {table}
            FROM '{s3_object}'
            IAM_ROLE '{iam_role}'
            JSON '{json_path}'
            REGION '{region}';
            """
        self.sql_params = {
            "table": table,
            "iam_role": iam_role,
            "json_path": json_path,
            "region": region,
        }
        self.s3_object = s3_object
        self.template_s3_object = template_s3_object
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Running StageToRedshiftOperator")
        if self.template_s3_object:
            s3_object = self.s3_object.format(
                year=context["execution_date"].strftime("%Y"),
                month=context["execution_date"].strftime("%m"),
            )
        else:
            s3_object = self.s3_object
        templated_sql_query = self.sql_query.format(
            s3_object=s3_object, **self.sql_params
        )

        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info("Running the SQL query on Redshift")
        redshift.run(templated_sql_query)
