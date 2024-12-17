import airflow
from airflow import DAG
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

partitioning_clickstream_job = SparkSubmitOperator(
    task_id="partitioning_clickstream_job",
    conn_id="spark-conn",
    application="jobs/python/partitioning_clickstream.py",
    application_args=[
        '2',
        '/opt/data/sample/clickstream',      # input_path
        '/opt/data/bronze/clickstream',      # output_path
        '\t',                                # delimiter
        'overwrite',                         # mode
        'true'                               # is_header
    ],
    dag=dag
)

partitioning_articles_job = SparkSubmitOperator(
    task_id="partitioning_articles_job",
    conn_id="spark-conn",
    application="jobs/python/partitioning_articles.py",
    application_args=[
        '2',
        '/opt/data/sample/articles',         # input_path
        '/opt/data/bronze/articles'          # output_pat
    ],
    dag=dag
)

cleaning_clickstream_job = SparkSubmitOperator(
    task_id="cleaning_clickstream_job",
    conn_id="spark-conn",
    application="jobs/python/cleaning_clickstream.py",
    application_args=[
        '/opt/data/bronze/clickstream',      # input_path
        '/opt/data/silver/clickstream',      # output_path
        'true',                              # is_header
        'overwrite'                          # mode
    ],
    dag=dag
)

cleaning_articles_job = SparkSubmitOperator(
    task_id="cleaning_articles_job",
    conn_id="spark-conn",
    application="jobs/python/cleaning_articles.py",
    application_args=[
        '/opt/data/bronze/articles',         # input_path
        '/opt/data/silver/articles',         # output_path
        'overwrite'                          # mode
    ],
    dag=dag
)

most_frequent_ids_job = SparkSubmitOperator(
    task_id="most_frequent_ids_job",
    conn_id="spark-conn",
    application="jobs/python/most_frequent_ids_job.py",
    application_args=[
        '/opt/data/silver/clickstream',      # path to clickstream
    ],
    dag=dag
)

long_tail_job = SparkSubmitOperator(
    task_id="long_tail_job",
    conn_id="spark-conn",
    application="jobs/python/long_tail_job.py",
    application_args=[
        '/opt/data/silver/clickstream',      # path to clickstream
    ],
    dag=dag
)

article_lengths_job = SparkSubmitOperator(
    task_id="article_lengths_job",
    conn_id="spark-conn",
    application="jobs/python/article_lengths_job.py",
    application_args=[
        '/opt/data/silver/clickstream',      # path to clickstream
        '/opt/data/silver/articles'          # path to articles
    ],
    dag=dag
)

categories_crossings_jobs = SparkSubmitOperator(
    task_id="categories_crossings_jobs",
    conn_id="spark-conn",
    application="jobs/python/categories_crossings_jobs.py",
    application_args=[
        '/opt/data/silver/clickstream',      # path to clickstream
        '/opt/data/silver/articles'          # path to articles
    ],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check

partitioning_tasks = [partitioning_clickstream_job, partitioning_articles_job]
cleaning_tasks = [cleaning_clickstream_job, cleaning_articles_job]
final_tasks = [article_lengths_job, most_frequent_ids_job, categories_crossings_jobs, long_tail_job]

health_check >> partitioning_tasks

for partitioning_task in partitioning_tasks:
    partitioning_task >> cleaning_tasks

for cleaning_task in cleaning_tasks:
    for final_task in final_tasks:
        cleaning_task >> final_task

final_tasks >> end
