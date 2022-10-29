from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'naver',
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
    description='Subjective Text ETL',
    schedule=timedelta(days=1),
    start_date=datetime(2022, 10, 26, 13, 10),
    catchup=False,
    tags=['naver_etl'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators


    t1 = BashOperator(
        task_id='extract_naver_jisik',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py extract naver_jisik',
        dag=dag
    )

    t2 = BashOperator(
        task_id='transform_subjective_text',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py transform subjective_tf',
        dag=dag
    )

    t3 = BashOperator(
        task_id='save_each_departments_qus_qty',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save qty_eachgwa',
        dag=dag
    )

    t4 = BashOperator(
        task_id='save_answer_each_day',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save answer_delay',
        dag=dag
    )

    dpt_1 = BashOperator(
        task_id='save_sub_rec_int_med',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save int_med_rec',
        dag=dag
    )

    dpt_2 = BashOperator(
        task_id='save_sub_rec_orc',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save ort_rec',
        dag=dag
    )

    dpt_3 = BashOperator(
        task_id='save_sub_rec_obstetrics',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save obstetrics_rec',
        dag=dag
    )

    dpt_4 = BashOperator(
        task_id='save_sub_rec_derma',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save derma_rec',
        dag=dag
    )

    dpt_5 = BashOperator(
        task_id='save_sub_rec_ent',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save ent_rec',
        dag=dag
    )

    dpt_6 = BashOperator(
        task_id='save_sub_rec_neuro',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save neuro_rec',
        dag=dag
    )

    dpt_7 = BashOperator(
        task_id='save_sub_rec_urology',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save urology_rec',
        dag=dag
    )

    dpt_8 = BashOperator(
        task_id='save_sub_rec_ophthal',
        cwd='/home/worker/project/hospital_etl',
        bash_command='python3 main.py save ophthal_rec',
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

    t1 >> t2 >> [t3, t4, dpt_1, dpt_2, dpt_3, dpt_4, dpt_5, dpt_6, dpt_7, dpt_8]