"""
Enterprise ETL Pipeline DAG
Migrated from legacy ETL to modern ELT architecture using dbt + Airflow
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'snowflake_conn_id': 'snowflake_default'
}

dag = DAG(
    'enterprise_etl_pipeline',
    default_args=default_args,
    description='Enterprise ETL Pipeline: Legacy to Modern ELT Migration',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'dbt', 'snowflake', 'production']
)

# Task 1: Extract raw data from source systems
extract_source_data = SnowflakeOperator(
    task_id='extract_source_data',
    sql="""
    -- Extract customers data
    COPY INTO @raw_data_stage/customers/
    FROM (
        SELECT customer_id, first_name, last_name, email, phone, 
               registration_date, status, created_at, updated_at
        FROM source_system.customers
        WHERE updated_at >= CURRENT_DATE - 1
    )
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    
    -- Extract orders data
    COPY INTO @raw_data_stage/orders/
    FROM (
        SELECT order_id, customer_id, order_date, order_status, 
               total_amount, currency, payment_method, shipping_address,
               created_at, updated_at
        FROM source_system.orders
        WHERE updated_at >= CURRENT_DATE - 1
    )
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """,
    dag=dag
)

# Task 2: Load raw data into Snowflake staging
load_raw_data = SnowflakeOperator(
    task_id='load_raw_data',
    sql="""
    -- Load customers into raw_data schema
    COPY INTO raw_data.customers
    FROM @raw_data_stage/customers/
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'ABORT_STATEMENT';
    
    -- Load orders into raw_data schema
    COPY INTO raw_data.orders
    FROM @raw_data_stage/orders/
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'ABORT_STATEMENT';
    """,
    dag=dag
)

# Task 3: Run dbt transformations (staging layer)
dbt_staging = BashOperator(
    task_id='dbt_staging',
    bash_command='cd /opt/airflow/dbt && dbt run --select staging.* --profiles-dir .',
    dag=dag
)

# Task 4: Run dbt data quality tests
dbt_test_staging = BashOperator(
    task_id='dbt_test_staging',
    bash_command='cd /opt/airflow/dbt && dbt test --select staging.* --profiles-dir .',
    dag=dag
)

# Task 5: Run dbt intermediate transformations
dbt_intermediate = BashOperator(
    task_id='dbt_intermediate',
    bash_command='cd /opt/airflow/dbt && dbt run --select intermediate.* --profiles-dir .',
    dag=dag
)

# Task 6: Run dbt marts (final analytics tables)
dbt_marts = BashOperator(
    task_id='dbt_marts',
    bash_command='cd /opt/airflow/dbt && dbt run --select marts.* --profiles-dir .',
    dag=dag
)

# Task 7: Run final data quality tests
dbt_test_marts = BashOperator(
    task_id='dbt_test_marts',
    bash_command='cd /opt/airflow/dbt && dbt test --select marts.* --profiles-dir .',
    dag=dag
)

# Task 8: Generate dbt documentation
dbt_docs = BashOperator(
    task_id='dbt_docs',
    bash_command='cd /opt/airflow/dbt && dbt docs generate --profiles-dir .',
    dag=dag
)

# Task 9: Update data freshness metrics
update_metrics = SnowflakeOperator(
    task_id='update_data_freshness_metrics',
    sql="""
    INSERT INTO analytics.data_freshness_metrics (
        table_name, 
        last_updated_at, 
        record_count,
        pipeline_run_id
    )
    SELECT 
        'fct_orders' as table_name,
        MAX(dbt_loaded_at) as last_updated_at,
        COUNT(*) as record_count,
        '{{ ds }}' as pipeline_run_id
    FROM analytics.fct_orders;
    """,
    dag=dag
)

# Task 10: Send success notification
send_notification = BashOperator(
    task_id='send_success_notification',
    bash_command='echo "Pipeline completed successfully at $(date)"',
    dag=dag
)

# Define task dependencies
extract_source_data >> load_raw_data >> dbt_staging >> dbt_test_staging
dbt_test_staging >> dbt_intermediate >> dbt_marts >> dbt_test_marts
dbt_test_marts >> dbt_docs >> update_metrics >> send_notification

