from airflow.sdk import dag,task,asset
from pendulum import datetime
import os
@asset(
    schedule = "@daily",
    # This is optional but good to include for clarity
    uri = "/opt/airflow/logs/data/data_extract.txt",
    name = "fetched_data"
)
def detch_data(self):
    os.makedirs(os.path.dirname(self.uri),exist_ok = True)
    with open(self.uri,"w") as f:
        f.write(f"Data fetched on successfully")
    print(f"Data Written to {self.uri}")
