from airflow.sdk import dag,task

@dag(
        dag_id="XCOMs_dag"
)
def dag_with_xcoms():
    @task.python
    def first_task():
        print("Extracting data... This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}
        return fetched_data
    @task.python
    def second_task(data:dict):
        fetched_data = data["data"]
        print("Transforming data... this is the second task")
        transformed_data_dict = {"trans_data":fetched_data*2}
        return transformed_data_dict

    @task.python
    def third_task(data:dict):
        print("Loading data... This is the third task DAG Complete")
        load_data = data["trans_data"]
        return load_data
    
    # Defining task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)
    first >> second >> third
# Instantiating the Dag
dag_with_xcoms()


















