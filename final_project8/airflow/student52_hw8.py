from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

with DAG(
        dag_id='student52_hw8',
        schedule_interval='0 */2 * * *',
        start_date=days_ago(0, hour=6),
        default_args={'retries': 0},
        max_active_runs=1,
        catchup=True,
) as dag:
    # Константы
    CSV_FILEPATH = '/var/data/*.csv'
    HDFS_FILEPATH = '/user/student52/'
    SCHEMA = 'student52'
    DATAMART_TABLE_NAME = 'datamart'
    WIDE_TABLE_NAME = 'wide_table'
    SPARK_APP = '/opt/airflow/dags/scripts/st52-jar-with-dependencies.jar'
    CONN = BaseHook.get_connection('postgres_student')
    JDBC_URI = f'jdbc:postgresql://{CONN.host}:{CONN.port}/testdb'
    TS = '{{ ts }}'


    def create_spark_submit_operator(table_name):
        return SparkSubmitOperator(
            task_id=f'create_{table_name}_table',
            conn_id='spark_cluster',
            name=f'create_{table_name}_table_[st52]',
            java_class='homework8.DownloadPGTables',
            application=SPARK_APP,
            application_args=[JDBC_URI, SCHEMA, table_name, CONN.login, CONN.password, TS]
        )


    create_pg_schema = SQLExecuteQueryOperator(
        task_id=f'create_{SCHEMA}_pg_schema',
        conn_id='postgres_student',
        sql=f'CREATE SCHEMA IF NOT EXISTS {SCHEMA};'
    )

    copy_csv_to_hdfs = BashOperator(
        task_id=f'copy_csv_files_to_hive',
        bash_command=f'hdfs dfs -put -f {CSV_FILEPATH} {HDFS_FILEPATH}'
    )

    create_account = SparkSubmitOperator(
        task_id=f'create_account_table',
        conn_id='spark_cluster',
        name='create_account_table_[st52]',
        java_class='homework8.DownloadCsvFiles',
        application=SPARK_APP,
        application_args=[HDFS_FILEPATH, SCHEMA, 'account', TS]
    )

    create_card = create_spark_submit_operator('card')
    create_person = create_spark_submit_operator('person')
    create_person_address = create_spark_submit_operator('person_adress')  # NOQA

    create_wide_table = SparkSubmitOperator(
        task_id=f'create_{WIDE_TABLE_NAME}',
        conn_id='spark_cluster',
        name=f'create_{WIDE_TABLE_NAME}_[st52]',
        java_class='homework8.CombineWideTable',
        application=SPARK_APP,
        application_args=[SCHEMA, WIDE_TABLE_NAME]
    )

    distribute_data_mart = SparkSubmitOperator(
        task_id=f'distribute_{DATAMART_TABLE_NAME}_table',
        conn_id='spark_cluster',
        name=f'distribute_{DATAMART_TABLE_NAME}_table_[st52]',
        java_class='homework8.DistributeDataMart',
        application=SPARK_APP,
        application_args=[JDBC_URI, SCHEMA, WIDE_TABLE_NAME, DATAMART_TABLE_NAME, CONN.login, CONN.password, TS]
    )

[copy_csv_to_hdfs >> create_account, create_pg_schema,
 create_card, create_person, create_person_address] >>\
 create_wide_table >> distribute_data_mart
