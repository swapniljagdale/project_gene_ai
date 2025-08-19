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

    # Define the steps to be executed on EMR
    SPARK_STEPS = [
        {
            'Name': 'inbound_preprocess_lm_onboarding',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Preprocess/inbound_preprocess_lm_onboarding.py'
                ],
            },
        },
        {
            'Name': 'preprocess_lnd_child_attributes_daily',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Landing/preprocess_lnd_child_attributes_daily.py'
                ],
            },
        },
        {
            'Name': 'preprocess_lnd_address_file_daily',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Landing/preprocess_lnd_address_daily.py'
                ],
            },
        },
        {
            'Name': 'preprocess_lnd_hcp_master_daily',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Landing/preprocess_lnd_hcp_master_daily.py'
                ],
            },
        },
        {
            'Name': 'preprocess_lnd_participants_daily',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Landing/preprocess_lnd_participant_daily.py'
                ],
            },
        },
        {
            'Name': 'preprocess_lnd_events_daily',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Landing/preprocess_lnd_event_daily.py'
                ],
            },
        },
        {
            'Name': 'normalize_participants',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Normalized/process_participants.py'
                ],
            },
        },
        {
            'Name': 'normalize_events',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Normalized/process_events.py'
                ],
            },
        },
        {
            'Name': 'generate_outbound',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Export/export_latest_data.py'
                ],
            },
        },
        {
            'Name': 'generate_summaries',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://your-code-bucket/src/Summarized/generate_kpis.py'
                ],
            },
        },
    ]

    # Create EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    # Add steps to EMR
    add_steps = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    # Monitor each step
    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    # Terminate EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    # Success email
    email_success = EmailOperator(
        task_id='email_success',
        to='your-email@example.com',
        subject='Events Daily Processing - Success',
        html_content="""
        <h3>Events Daily Processing Completed Successfully</h3>
        <p>The daily events processing pipeline has completed successfully.</p>
        <p>Processed on: {{ ds }}</p>
        """,
    )

    # Failure email
    email_failure = EmailOperator(
        task_id='email_failure',
        to='your-email@example.com',
        subject='Events Daily Processing - Failed',
        html_content="""
        <h3>Events Daily Processing Failed</h3>
        <p>The daily events processing pipeline has failed.</p>
        <p>Failed on: {{ ds }}</p>
        <p>Error: {{ exception }}</p>
        """,
        trigger_rule='one_failed',
    )

    # Set up task dependencies
    create_emr_cluster >> add_steps >> step_checker >> terminate_emr_cluster >> email_success
    [create_emr_cluster, add_steps, step_checker, terminate_emr_cluster] >> email_failure
