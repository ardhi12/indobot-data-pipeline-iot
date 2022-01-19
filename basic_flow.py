from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG configuration
with DAG("basic_flow", start_date=datetime(2022, 1, 1), schedule_interval="48 13 * * *", catchup=False) as dag:

    # tasks configuration    
    start = DummyOperator(
        task_id="start"            
    )
    
    proses_1 = BashOperator(
        task_id="process_1",
        bash_command="sleep 10"            
    )

    proses_2 = BashOperator(
        task_id="process_2",
        bash_command="sleep 5"            
    )
    
    finish = DummyOperator(
        task_id="finish"            
    )

    # set workflow
    start >> proses_1 >> proses_2 >> finish