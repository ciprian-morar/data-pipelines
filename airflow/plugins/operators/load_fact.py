from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# """LoadFactOperator it's used to create DW Fact tables and 
# insert the data from staging tables"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
#     '''Constructor LoadFactOperator
#        Parameters:
#             redshift_conn_id (string): the name of connection
#             table (string): the name of the table
#             create_table (string): The SQL query which create the table
#             insert_table (string): The SELECT statement from staging tables
#             delete_flag (boolean): The flag allows to switch between append-only 
#                                     and delete-load functionality
#     '''
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                redshift_conn_id,
                table,
                create_table,
                insert_table,
                delete_flag,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table = create_table
        self.insert_table = insert_table
        self.delete_flag = delete_flag
        

    def execute(self, context):
        self.log.info('LoadFactOperator')
        #connect to redshift with the PostgresHook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        table_insert = f"""
            INSERT INTO {self.table}
            {self.insert_table}
        """
#         """The flag allows to switch between append-only 
#            and delete-load functionality
#         """
        if self.delete_flag:
            delete_statement = f'DROP TABLE IF EXISTS {self.table}'
            # run delete table statement
            redshift_hook.run(delete_statement)
        # run create table statement    
        redshift_hook.run(self.create_table)
        # run insert table statement
        redshift_hook.run(table_insert)
