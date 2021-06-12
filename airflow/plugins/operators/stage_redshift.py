from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Operator to load JSON data from Amazon S3 into Amazon Redshift
    :param redshift_conn_id: Connection id of the Redshift connection to use
        Default is 'redshift'
    :type redshift_conn_id: str

    :param aws_credentials_id: Connection id of the AWS credentials to use to access S3 data
    :type aws_credentials_id: str
        Default is 'aws_credentials'
    :param table_name: Redshift staging table name
    :type table_name: str
    :param s3_bucket: Amazon S3 bucket where staging data is read from
    :type s3_bucket: str
    :param json_path: Amazon S3 bucket JSON path
    :type json_path: str
        Default is 'auto'
    :param use_partitioning: If true, S3 data will be loaded as partitioned data based on year and month of execution_date
    :type use_partitioning: bool
        Default is 'False'
    :param execution_date: Logical execution date of DAG run (templated)
    :type execution_date: str

    """

    copy_sql_date = """
        COPY {}
        FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 table = "",
                 s3_path = "",
                 json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        # Backfill a specific date
        if self.execution_date:
            formatted_sql = StageToRedshiftOperator.copy_sql_time.format(
                self.table,
                self.s3_path,
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key,
                self.json_path,
                self.execution_date
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path,
                self.execution_date
            )

        redshift.run(formatted_sql)
