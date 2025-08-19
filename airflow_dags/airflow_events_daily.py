"""
Airflow DAG for daily events processing pipeline.
This DAG orchestrates the ETL process for event data using AWS EMR.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'airflow_events_daily',
    default_args=default_args,
    description='Daily processing of events data using EMR',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=days_ago(1),
    tags=['events', 'emr', 'daily'],
    catchup=False,
) as dag:
    
    # EMR configuration
    JOB_FLOW_OVERRIDES = {
        'Name': 'events-daily-processing',
        'ReleaseLabel': 'emr-6.8.0',
        'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Worker nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
        },
        'VisibleToAllUsers': True,
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
        'LogUri': 's3://your-log-bucket/emr-logs/',
    }

    # Create EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    # --- Define Steps ---

    # Step 1: Landing (assuming all landing scripts can run, but for simplicity we run one operator)
    # In a real scenario, you might have one operator per landing script if they can run in parallel.
    landing_step = EmrAddStepsOperator(
        task_id='run_landing_scripts',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[
            # Add all landing and preprocess scripts here
        ]
    )

    # Step 2: Normalization for Events and Participants (can run in parallel)
    normalize_events_step = EmrAddStepsOperator(
        task_id='normalize_events',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[{
            'Name': 'Normalize Events',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster', 's3://your-code-bucket/src/Normalized/process_events.py']
            }
        }]
    )

    normalize_participants_step = EmrAddStepsOperator(
        task_id='normalize_participants',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[{
            'Name': 'Normalize Participants',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster', 's3://your-code-bucket/src/Normalized/process_participants.py']
            }
        }]
    )

    # Step 3: Normalization for Expenses (depends on events)
    normalize_expenses_step = EmrAddStepsOperator(
        task_id='normalize_expenses',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[{
            'Name': 'Normalize Expenses',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster', 's3://your-code-bucket/src/Normalized/process_expenses.py']
            }
        }]
    )

    # Step 4: Summarization and Export (depend on all normalization)
    generate_kpis_step = EmrAddStepsOperator(
        task_id='generate_kpis',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[{
            'Name': 'Generate KPIs',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster', 's3://your-code-bucket/src/Summarized/generate_kpis.py']
            }
        }]
    )

    export_data_step = EmrAddStepsOperator(
        task_id='export_latest_data',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[{
            'Name': 'Export Latest Data',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster', 's3://your-code-bucket/src/Outbound/export_latest_data.py']
            }
        }]
    )
    
    # Step 5: Step checker to wait for completion
    step_checker = EmrStepSensor(
        task_id='watch_last_step',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='export_latest_data', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    # Step 6: Terminate EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        trigger_rule='all_done', # Run whether upstream steps succeed or fail
    )

    # --- Task Dependencies ---
    create_emr_cluster >> landing_step
    landing_step >> [normalize_events_step, normalize_participants_step]
    normalize_events_step >> normalize_expenses_step
    [normalize_expenses_step, normalize_participants_step] >> generate_kpis_step
    [normalize_expenses_step, normalize_participants_step] >> export_data_step
    [generate_kpis_step, export_data_step] >> step_checker >> terminate_emr_cluster
