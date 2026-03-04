from airflow.sdk import dag,task

@dag(
        dag_id="dag_versioning"
)
def dag_versioning():
    @task.python
    def first_task():
        print("This is the first task")
    @task.python
    def second_task():
        print("this is the second task")
    @task.python
    def third_task():
        print("This is the third task DAG Complete")
    @task.python
    def version_task():
        print("This is the version task DAG version 2.0!!!!")
    @task.python
    def version_task2():
        print("This is the version task DAG version 2.022222!!!!")
    
    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    version = version_task()
    version2 = version_task2()
    first >> second >> third >> version >> version2
# Instantiating the Dag
dag_versioning()


















