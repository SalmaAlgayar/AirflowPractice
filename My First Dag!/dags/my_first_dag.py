from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(dag_id = "my_first_dag",
         description = "This is a dag for getting my hands dirty",
         start_date=datetime(2025,7,12,22,50),
         schedule= "*/5 * * * *",
         catchup= False, dagrun_timeout=timedelta(minutes=45),
         tags=["practice", "daily"]) as dag:

    create_table = SQLExecuteQueryOperator(
            task_id = "create_customers_table",
            conn_id = "postgres_conn",
            sql = r"/sql/create.sql"
            )

    insert_values = SQLExecuteQueryOperator(
            task_id = "insert_customers_values",
            conn_id = "postgres_conn",
            sql = r"/sql/insert.sql"
            )

    create_table >> insert_values
