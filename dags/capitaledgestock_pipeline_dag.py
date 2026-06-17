# import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from capitaledgestock_pipeline import run_pipeline

# Define default arguments for the DAG
with DAG(
    dag_id='capitaledgestockprices_pipeline',
    start_date=datetime(2026, 6, 16),
    schedule='@daily',
    catchup=False,
    tags=['capitaledge', 'stockprices'],
) as dag:
    
    t1 = PythonOperator(
        task_id='run_stock_pipeline',
        python_callable=run_pipeline,
        retries=3,  # Retry up to 3 times if the task fails
        retry_delay=timedelta(minutes=5),  # Wait 5 minutes between retries
    )

    #creating a function for validateing the data after loading into database
    t2 = BashOperator(
        task_id='validate_data',
        bash_command='echo "Data validation step - check database for new records"'
    )
    


    