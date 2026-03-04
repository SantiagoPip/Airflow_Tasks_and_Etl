from airflow.sdk import dag,task
from airflow.operators.bash import BashOperator
@dag(
        dag_id="operators_dag"
)
def operators_dag():
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
    @task.bash
    def bash_task():
        return "echi https://airflow.apache.org/"
    bash_task_oldschool = BashOperator(
        task_id ="bash_task_oldschool",
        bash_command ="echo https://airflow.apache.org/"
    )
    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    version = version_task()
    version2 = version_task2()
    bash_oldschool = bash_task_oldschool
    first >> second >> third >> version >> version2 >> bash_oldschool
   
# Instantiating the Dag
operators_dag()


















