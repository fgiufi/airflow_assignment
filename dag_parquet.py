from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dag_data_processing import DataProcessing

default_args = {
    'owner': 'sychen',
    'start_date': datetime(2022, 9, 13),
    'retries': 0,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'execution_date': '{execution_date}',
}

dag = DAG('convert_csv_to_parquet',
          default_args=default_args,
          schedule_interval='00 23 * * *',
          )
start_read_data = PythonOperator(task_id='start_read_data',
                                         python_callable=DataProcessing.print_parquet_massage,
                                         dag=dag)

# etl  bi
parquet_operator_ProductData = PythonOperator(
    task_id='read_product_data',
    python_callable=DataProcessing.convert_csv_to_parquet_product_data,
    op_kwargs={
        "raw_path": "raw/{{ ds }}/",
        "parquet_path": "parquet_file/{{ ds }}/",
        "processed_path": "processed_parquet/{{ ds }}/"
    },
    dag=dag
)

parquet_operator_SalesOrderData = PythonOperator(
    task_id='read_sales_order_data',
    python_callable=DataProcessing.convert_csv_to_parquet_sales_order_data,
    op_kwargs={
        "raw_path": "raw/{{ ds }}/",
        "parquet_path": "parquet_file/{{ ds }}/",
        "processed_path": "processed_parquet/{{ ds }}/"
    },
    dag=dag
)

parquet_operator_customer_address = PythonOperator(
    task_id='read_customer_address',
    python_callable=DataProcessing.convert_csv_to_parquet_customer_address,
    op_kwargs={
        "raw_path": "raw/{{ ds }}/",
        "parquet_path": "parquet_file/{{ ds }}/",
        "processed_path": "processed_parquet/{{ ds }}/"
    },
    dag=dag
)

parquet_operator_address_data = PythonOperator(
    task_id='read_address_data',
    python_callable=DataProcessing.convert_csv_to_parquet_address_data,
    op_kwargs={
        "raw_path": "raw/{{ ds }}/",
        "parquet_path": "parquet_file/{{ ds }}/",
        "processed_path": "processed_parquet/{{ ds }}/"
    },
    dag=dag
)

python_operator_processing = PythonOperator(task_id='start_data_processing',
                                            python_callable=DataProcessing.print_data_processing_massage,
                                            dag=dag)

operator_sales_profit_diff_cities = PythonOperator(
    task_id='sales_profit_diff_cities',
    python_callable=DataProcessing.sales_profit_diff_cities,
    op_kwargs={
        "execution_date": "{{ ds }}",
        "raw_path": "raw/{{ ds }}/",
        "parquet_path": "parquet_file/{{ ds }}/",
        "processed_path": "processed_parquet/{{ ds }}/"
    },
    dag=dag
)

operator_total_profit_top10 = PythonOperator(
    task_id='total_profit_top10',
    python_callable=DataProcessing.total_profit_top10,
    op_kwargs={
        "execution_date": "{{ ds }}",
        "raw_path": "raw/{{ ds }}/",
        "parquet_path": "parquet_file/{{ ds }}/",
        "processed_path": "processed_parquet/{{ ds }}/"
    },
    dag=dag
)

operator_Longest_time_span = PythonOperator(
    task_id='Longest_time_span',
    python_callable=DataProcessing.Longest_time_span,
    op_kwargs={
        "execution_date": "{{ ds }}",
        "raw_path": "raw/{{ ds }}/",
        "parquet_path": "parquet_file/{{ ds }}/",
        "processed_path": "processed_parquet/{{ ds }}/"
    },
    dag=dag
)

start_read_data >> [parquet_operator_ProductData, parquet_operator_SalesOrderData,
                            parquet_operator_customer_address,
                            parquet_operator_address_data] >> python_operator_processing >> [
    operator_sales_profit_diff_cities, operator_total_profit_top10, operator_Longest_time_span]
