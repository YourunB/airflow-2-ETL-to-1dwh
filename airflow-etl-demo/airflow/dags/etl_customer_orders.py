from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def etl_merge_plain():
    pg1 = PostgresHook(postgres_conn_id='pg_source1')
    pg2 = PostgresHook(postgres_conn_id='pg_source2')
    dwh = PostgresHook(postgres_conn_id='pg_dwh')

    # Получаем списки
    customers = pg1.get_records("SELECT id, name FROM customers")
    orders = pg2.get_records("SELECT customer_id, amount FROM orders")

    # Готовим объединённую агрегацию
    result = {}
    for cid, name in customers:
        total = sum(amount for oid, amount in orders if oid == cid)
        if total > 0:
            result[name] = total

    # Переводим в список кортежей
    rows = [(name, total) for name, total in result.items()]
    dwh.insert_rows(table='customer_orders_summary', rows=rows)

with DAG('etl_merge_plain', start_date=datetime(2024,1,1), schedule_interval=None, catchup=False) as dag:
    PythonOperator(
        task_id='merge_load_plain',
        python_callable=etl_merge_plain
    )
