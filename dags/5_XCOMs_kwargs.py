from airflow.sdk import dag,task

@dag(
        dag_id="XCOMs_da_manual"
)
def dag_with_xcoms_manual():
    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        print("Extracting data... This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}
        ti.xcom_push(key='return_result',value=fetched_data)
    @task.python
    def second_task(**kwargs):     
        ti= kwargs['ti']   
        fetched_data = ti.xcom_pull(task_ids="first_task",key='return_result')['data']
        print("Transforming data... this is the second task")
        transformed_data_dict = {"trans_data":fetched_data*2}
        ti.xcom_push(key="transf_data",value=transformed_data_dict)
    @task.python
    def third_task(**kwargs):
        ti=kwargs['ti']
        print("Loading data... This is the third task DAG Complete")
        load_data = ti.xcom_pull(task_ids="second_task",key="transf_data")
        return load_data
    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    first >> second >> third
# Instantiating the Dag
dag_with_xcoms_manual()


















