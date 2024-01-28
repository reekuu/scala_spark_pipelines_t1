from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from hdfs import InsecureClient

# Constants
TABLE_NAME = Variable.get('student52_table_name')
HDFS_URL = 'http://ca-oshspark-n-01.local:9870'
OUTPUT_FILEPATH = '/output/st52.txt'


def read_txt_file_from_hdfs(hdfs_url, hdfs_dir):
    client = InsecureClient(hdfs_url)
    txt_file = next(file for file in client.list(hdfs_dir) if file.endswith('.txt'))
    with client.read(f"{hdfs_dir}/{txt_file}", encoding='utf-8') as reader:
        return reader.read().strip()


dag = DAG(
    dag_id='student52_hw6',
    schedule_interval='0 */4 * * *',
    start_date=days_ago(0, hour=4),
    default_args={'retries': 0},
    max_active_runs=1,
    catchup=False,
)

check_table_existence = HiveOperator(
    task_id='check_table_existence',
    hql=f'SELECT * FROM {TABLE_NAME} LIMIT 1',
    hive_cli_conn_id='hive_student',
    dag=dag,
)

send_email_missing_table = EmailOperator(
    task_id='send_email_missing_table',
    to='student52@gmail.com',
    subject=f'Table {TABLE_NAME} Missing in Hadoop',
    html_content='The specified table does not exist in Hadoop.',
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag,
)

count_table_rows = SparkSubmitOperator(
    task_id='count_table_rows',
    conn_id='spark_cluster',
    java_class='homework6.CountTableRows',
    application='/opt/airflow/dags/scripts/st52-jar-with-dependencies.jar',
    application_args=[TABLE_NAME, OUTPUT_FILEPATH],
    trigger_rule=TriggerRule.ALL_SKIPPED,
    dag=dag,
)

transfer_count = PythonOperator(
    task_id='transfer_count',
    python_callable=read_txt_file_from_hdfs,
    op_args=[HDFS_URL, OUTPUT_FILEPATH],
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

send_email_row_count = EmailOperator(
    task_id='send_email_row_count',
    to='student52@gmail.com',
    subject=f'Row Count of Hadoop Table {TABLE_NAME}',
    html_content='''<h3>Row Count:</h3><p>{{ task_instance.xcom_pull(task_ids='transfer_count') }}</p>''',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

check_table_existence >> send_email_missing_table >> count_table_rows >> transfer_count >> send_email_row_count
