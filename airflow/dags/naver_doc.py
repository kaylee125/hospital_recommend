from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'naver_doc',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['lms4678@naver.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=20),
    },
    description='New DOC EXTRACT',
    schedule=timedelta(days=7),
    start_date=datetime(2022, 10, 26, 9, 00),
    catchup=False,
    tags=['doc_etl'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators


    t1 = BashOperator(
        task_id='extract_doc_list',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py extract naver_doc',
        dag=dag
    )

    t2 = BashOperator(
        task_id='append_doc_list',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py transform naver_doc_tf',
        dag=dag
    )





    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t1 >> t2