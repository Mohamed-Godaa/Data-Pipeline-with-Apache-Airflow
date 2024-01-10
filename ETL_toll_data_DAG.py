# imports
from datetime import datetime, timedelta

from airflow import DAG, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Dag arguments
default_args = {
    'owner' : 'the best DAGger',
    'start_date' : datetime.today(),
    'email' : 'thebestdagger@dag.ai',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5)
}


# DAG Deifinition

dag = DAG(
    dag_id = 'ETL_toll_data',
    schedule_interval= timedelta(days=1),
    default_args = default_args,
    description = 'Apache Airflow Final Assignment'
)

unzip_data = BashOperator(
    task_id = 'unzip_data',
    dag = dag,
    bash_command = 'tar -xvf tolldata.tgz'
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    dag = dag,
    bash_command = '''
    echo "starting extracxt data from csv file"

    cut -d"," -f1-4 vehicle-data.csv > csv_data.csv

    '''
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    dag = dag,
    bash_command = """
    echo 'starting extracxt data from tsv file'

    cut -d'\t' -f5-7 tollplaza-data.tsv > extracted_tsv_data.tsv

    tr '\t' ',' < extracted_tsv_data.tsv > tsv_data.csv

    """
)

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    dag = dag,
    bash_command = """
    echo 'starting extracxt data from txt file'

    cut -d' ' -f6-7 payment-data.txt > extracted_txt_data.txt

    tr ' ' ',' < extracted_txt_data.txt > fixed_width_data.csv

    """
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    dag = dag,
    bash_command = """
    echo 'Consolidate data from all 3 files'

    paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv

    """
)

transform_data = BashOperator(
    task_id = 'transform_data',
    dag = dag,
    bash_command = """
    echo 'Transforming vehicle_type from extracted_data file'

    tr "[a-z]" "[A-Z]" < cut -d"," -f4 extracted_data.csv > staging/transformed_data.csv

    """
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data