from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_string():
    print("HALO! Saya dijalankan menggunakan PythonOperator")

# DAG configuration
with DAG("basic_flow", start_date=datetime(2022, 1, 1), schedule_interval="48 13 * * *", catchup=False) as dag:

    # tasks configuration    
    start = DummyOperator(
        task_id="start"            
    )

    proses = DummyOperator(
        task_id="proses"            
    )
    
    # proses_1 = BashOperator(
    #     task_id="process_1",
    #     bash_command="sleep 10"            
    # )

    # proses_2 = PythonOperator(
    #     task_id         = "proses_2",
    #     python_callable = print_string    
    # )
    
    finish = DummyOperator(
        task_id="finish"            
    )

    # set workflow
    start >> proses >> finish
    # start >> proses_1 >> proses_2 >> finish