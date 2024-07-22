from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

class PostgreSQLCountRows(SQLExecuteQueryOperator):
    table_name = 'table_name'

    def __init__(self, table_name: str, conn_id: str, *args, **kwargs):
        super().__init__(
            sql=f"SELECT COUNT(*) FROM {table_name};",
            conn_id=conn_id,
            *args,
            **kwargs
        )
        self.table_name = table_name
        self.conn_id = conn_id

    def execute(self, context):
        self.log.info(f"Counting rows in table {self.table_name}")
        result = super().execute(context)
        count = result[0][0]
        self.log.info(f"Number of rows in table {self.table_name}: {count}")
        return count
