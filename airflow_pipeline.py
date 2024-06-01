import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'

default_args = {
    "owner": "snowflakedatapipelinepro",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag= DAG(
    "customer_orders_datapipeline_dynamic_batch_id"
    default_args=default_args,
    description="Runs data pipeline",
    schedule_interval=None,
    is_paused_upon_creation=False,
)

bash_task = Bashoperator(task_id="run_bash_echo", bash_command="echo 1", dag=dag)

post_task = Bashoperator(task_id="post_dbt", bash_command="echo 0", dag-dag)

batch_id =str(datetime.datetime.now().strftime("%Y%m%d%H%M"))
print ("BATCH_ID = " + batch_id)


task_customer_landing_to_processing = Bashoperator(
    task_id="customer_landing_to_processing",
    bash_command='aws s3 mv s3://snowflakedatapipelinepro/firehose/customers/landing s3://snowflakedatapipelinepro/firehose/customers/processing/{0}/ --recursive'.format(batch_id),
    dag-dag
)

task_customers_processing_to_processed = Bashoperator(
    task_id="customer_processing_to_processed",
    bash_command='aws s3 mv s3://snowflakedatapipelinepro/firehose/customers/processing/{0}/ s3://snowflakedatapipelinepro/firehose/customers/processed/{0}/ --recursive'.format(batch_id),
    dag-dag
)

task_orders_landing_to_processing = Bashoperator(
    task_id="orders_landing_to_processing"
    bash_command='aws s3 mv s3:// snowflakedatapipelinepro/firehose/orders/landing/ s3://snowflakedatapipelinepro/firehose/customers/processing/{0}/ --recursive'.format(batch_id),
    dag-dag
)

task_orders_processing_to_processed = Bashoperator(
    task_id="orders_processing_to_processed"
    bash command='aws s3 mv s3://snowflakedatapipelinepro/firehose/orders/processing/{0}/ s3://snowflakedatapipelinepro/firehose/customers/processed/{0}/ --recursive'.format(batch_id),
    dag-dag
)

snowflake_query_orders = [
    """copy into PRO_DB.PRO_SCHEMA.ORDERS_RAW
(O_ ORDERKEY,O_CUSTKEY,O_ ORDERSTATUS,O_TOTALPRICE.O_ORDERDATE,O_ ORDERPRIORITY,O_CLERK.O
( select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,'{0}" from @ORDERS_RAW_STAGE t);""",
]

snowflake_query_customers = [
    """ copy into PRO_DB.PRO_SCHEMA.CUSTOMER_RAN
(C_CUSTKEY, C_NAME, C_ADDRESS, C_NATTONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT
( select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,'{0}' from @CUSTOMER_RAW_STAGE t);""",
]
