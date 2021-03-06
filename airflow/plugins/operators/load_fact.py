from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    Operator loads and transforms data from Redshift staging table to fact table based on parameters provided.
    :param redshift_conn_id: Connection id of the Redshift connection to use
        Default is 'redshift'
    :type redshift_conn_id: str
    :param fact_table_name: Redshift fact table name where data will be inserted
    :type fact_table_name: str
    :param fact_insert_columns: Redshift fact table column names for table where data will be inserted
    :type fact_insert_columns: str
    :param fact_insert_sql: Query representing data that will be inserted
    :type fact_insert_sql: str
    :param truncate_table: If True, data will be truncated from fact table prior to inserting.
    :type truncate_table: bool
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_only:
            self.log.info("Delete {} fact table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Insert data from staging tables into {} fact table".format(self.table))

        insert_statement = f"INSERT INTO {self.table} \n{self.sql}"
        self.log.info(f"Running sql: \n{insert_statement}")
        redshift.run(insert_statement)
        self.log.info(f"Successfully completed insert into {self.table}")
