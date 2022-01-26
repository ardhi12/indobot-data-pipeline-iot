import pandas as pd
from airflow import DAG
from datetime import datetime
from google.cloud import bigquery
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

# -------------------- Create BQ client --------------------
bq_hook   = BigQueryHook(bigquery_conn_id = 'conn_bq_iot', use_legacy_sql = False)
bq_client = bigquery.Client(project = bq_hook._get_field("project"), credentials = bq_hook._get_credentials())

# -------------------- Define table schema --------------------
schema = [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'temperature', 'type': 'FLOAT', 'mode': 'NULLABLE'}, 
        {'name': 'humidity', 'type': 'FLOAT', 'mode': 'NULLABLE'}, 
        {'name': 'device_status', 'type': 'INTEGER', 'mode': 'NULLABLE'}, 
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]

def convert_datatype(dataframe, schema) -> pd.DataFrame:
    """
    Transorm data type of dataframe based on BigQuery schema

    Params:
        dataframe   (str) | Required : The dataframe
        schema      (str) | Required : Schema of table bigquery

    Returns:
        Dataframe with the correct data type
    """
    for key in schema:
        name  = key["name"]
        dtype = key["type"]

        if "TIME" in dtype or "DATE" in dtype:
            dataframe[name] = dataframe[name].astype("str")
            dataframe[name] = dataframe[name].apply(lambda x: pd.to_datetime(x, errors="coerce"))
 
            if dtype == "DATE":
                dataframe[name] = dataframe[name].dt.date
            elif dtype == "TIME":
                print("EXAMPLE:", dataframe[name].iloc[0])
                dataframe[name] = dataframe[name].dt.time        
    
    return dataframe

def extract():
    # read csv file
    df = pd.read_csv('sources/iot_logs.csv')
    df = convert_datatype(df,schema)
    print("[INFO] read csv berhasil")

    return df

def load(ti):    
    # get df from extract_data task
    df = ti.xcom_pull(task_ids='extract_data')
    
    # define job config
    config = {
        "schema"           : schema,
        "write_disposition": "WRITE_TRUNCATE"
    }

    # set dataset and table
    dataset_ref  = bq_client.dataset('sample')
    table_ref    = dataset_ref.table('iot_logs')
    upload_to_bq = lambda arg: bq_client.load_table_from_dataframe(arg["df"], table_ref, job_config=arg["job_config"]).result()

    # truncate table
    upload_to_bq({"df": pd.DataFrame(columns=[sch["name"] for sch in schema]), "job_config": bigquery.LoadJobConfig(**config)})
    print("[INFO] truncate berhasil")    

    # load data to BQ
    upload_to_bq({"df": df, "job_config": bigquery.LoadJobConfig(**config)})
    print("[INFO] load to bq berhasil")
    

# DAG configuration
with DAG("iot_pipeline", start_date=datetime(2022, 1, 1), schedule_interval="0 8 * * *", catchup=False) as dag:

    # tasks configuration    
    start = DummyOperator(
        task_id = "start"            
    )

    extract_data = PythonOperator(
        task_id         = "extract_data",
        python_callable = extract               
    )
    
    load_to_bq = PythonOperator(
        task_id         = "load_to_bq",
        python_callable = load        
    )    
    
    finish = DummyOperator(
        task_id="finish"            
    )

    # set workflow
    start >> extract_data >> load_to_bq >> finish
