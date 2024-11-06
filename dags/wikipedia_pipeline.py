import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "wikipedia_pipeline",
    default_args = {
        "owner": "Damian Jakub",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = None
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

health_check = SparkSubmitOperator(
    task_id="health_check",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

partitioning_job = SparkSubmitOperator(
    task_id="partitioning_job",
    conn_id="spark-conn",
    application="jobs/python/partitioning.py",
        application_args=[
        '2',
        '/opt/data/sample',      # input_path
        '/opt/data/bronze',      # output_path
        '\t',                    # delimiter
        'overwrite',             # mode
        'true'                   # is_header
    ],
    dag=dag
)

cleaning_job = EmptyOperator(
    task_id="cleaning_job",
    dag=dag
)

normalization_job = EmptyOperator(
    task_id="normalization_job",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> partitioning_job >> [cleaning_job, normalization_job] >> end