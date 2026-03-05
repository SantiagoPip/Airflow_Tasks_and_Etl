from airflow.sdk import dag,task

@dag(
        dag_id="branches"
)
def branches_dag():
    @task.python
    def extract_data(**kwargs):
        print("Extracting data...")
        ti =kwargs["ti"]
        extracted_data  = {"api_extracted_data":[1,2,3],
                           "db_extracted_data":[4,5,6],
                           "s3_extracted_data":[7,8,9],
                           "weekend_flag":"true"}
        ti.xcom_push(key='return_value',value =extracted_data)
    @task.python
    def transform_data_api(**kwargs):
        ti = kwargs["ti"]
        api_data = ti.xcom_pull(task_ids="extract_data")["api_extracted_data"]
        transformed_api_data = [i*10 for i in api_data]
        ti.xcom_push(key = "return_value", value=transformed_api_data)
    @task.python
    def transform_data_db(**kwargs):
        ti = kwargs["ti"]
        api_data = ti.xcom_pull(task_ids="extract_data")["db_extracted_data"]
        transformed_db_data = [i*100 for i in api_data]
        ti.xcom_push(key = "return_value", value=transformed_db_data)
    @task.python
    def transform_data_s3(**kwargs):
        ti = kwargs["ti"]
        api_data = ti.xcom_pull(task_ids="extract_data")["s3_extracted_data"]
        transformed_s3_data = [i*1000 for i in api_data]
        ti.xcom_push(key = "return_value", value=transformed_s3_data)   
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs["ti"]
        weekend_flag = ti.xcom_pull(task_ids = "extract_data")["weekend_flag"]
        if weekend_flag == "true":
            return "no_load_task_weekend"
        else:
            return "load_data"
    @task.bash
    def load_data(**kwargs):
        print("load_data")
        api_data = kwargs["ti"].xcom_pull(task_ids="transform_data_api")
        db_data = kwargs["ti"].xcom_pull(task_ids="transform_data_db")
        s3_data = kwargs["ti"].xcom_pull(task_ids="transform_data_s3")
        return f"echo 'Loaded Data:{api_data}, {db_data},{s3_data}'"
    #create decider Node
    @task.bash
    def no_load_task_weekend(**kwargs):
        print("Not loading on weekends...")
        return f"echo 'Loaded Data:complete!!!'"

    # Defining task dependencies
    extract = extract_data()
    transform_api = transform_data_api()
    transform_db = transform_data_db()
    transform_s3 = transform_data_s3()
    load = load_data()
    no_load = no_load_task_weekend()
    extract >> [transform_api,transform_db,transform_s3] >> decider_task() >> [load,no_load]
# Instantiating the Dag
branches_dag()


















