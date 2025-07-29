from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

@dag(
    dag_id="etl_merge_plain",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl"]
)
def etl_merge_plain_dag():
    @task
    def etl_merge_plain():
        pg1 = PostgresHook(postgres_conn_id='pg_source1')
        pg2 = PostgresHook(postgres_conn_id='pg_source2')
        dwh = PostgresHook(postgres_conn_id='pg_dwh')

        customers = pg1.get_records("SELECT id, name FROM customers")
        orders = pg2.get_records("SELECT customer_id, amount FROM orders")

        result = {}
        for cid, name in customers:
            total = sum(amount for oid, amount in orders if oid == cid)
            if total > 0:
                result[name] = total

        rows = [(name, total) for name, total in result.items()]
        dwh.insert_rows(table='customer_orders_summary', rows=rows)

    etl_merge_plain()

dag = etl_merge_plain_dag()
