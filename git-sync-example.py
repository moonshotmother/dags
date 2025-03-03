from datetime import datetime, timedelta
import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagBag
from airflow.utils.db import provide_session
from airflow.models import DagModel

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 3),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Path to the DAG directory which is also the git repository
DAG_DIR = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER', '/media/dadams/airflow/dags')

def git_pull():
    """Pull the latest changes from the git repository"""
    try:
        # Change to the DAG directory
        os.chdir(DAG_DIR)
        
        # Execute git pull
        result = subprocess.run(['git', 'pull'], 
                               capture_output=True, 
                               text=True, 
                               check=True)
        
        # Log the output
        print(f"Git pull output: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error executing git pull: {e.stderr}")
        raise e

@provide_session
def reload_dags(session=None):
    """Force Airflow to reload all DAGs"""
    try:
        # Clear the DagBag cache
        dagbag = DagBag(dag_folder=DAG_DIR, include_examples=False)
        
        # Get the list of DAG files
        dag_files = [f for f in os.listdir(DAG_DIR) if f.endswith('.py')]
        
        # Count of successful reloads
        success_count = 0
        
        # Process each DAG file
        for dag_file in dag_files:
            try:
                # Get the full path to the DAG file
                dag_file_path = os.path.join(DAG_DIR, dag_file)
                
                # Reload the DAG
                dagbag.process_file(dag_file_path)
                
                # Check for errors
                if dag_file in dagbag.import_errors:
                    print(f"Error reloading DAG {dag_file}: {dagbag.import_errors[dag_file]}")
                else:
                    success_count += 1
                    print(f"Successfully reloaded DAG file: {dag_file}")
            except Exception as e:
                print(f"Exception while processing {dag_file}: {str(e)}")
        
        # Update the DAG metadata in the database
        for dag_id, dag in dagbag.dags.items():
            dag.sync_to_db(session=session)
            
        print(f"Successfully reloaded {success_count} out of {len(dag_files)} DAG files")
        return f"Successfully reloaded {success_count} out of {len(dag_files)} DAG files"
    except Exception as e:
        print(f"Error reloading DAGs: {str(e)}")
        raise e

# Define the DAG
dag = DAG(
    'git_pull_and_reload_dags',
    default_args=default_args,
    description='Pull git changes and reload all DAGs',
    schedule_interval=timedelta(minutes=10),  # Run every 10 minutes
    catchup=False,
    tags=['git', 'admin', 'maintenance'],
)

# Task to pull git changes
pull_changes = PythonOperator(
    task_id='pull_git_changes',
    python_callable=git_pull,
    dag=dag,
)

# Task to reload DAGs
reload_all_dags = PythonOperator(
    task_id='reload_all_dags',
    python_callable=reload_dags,
    dag=dag,
)

# Set task dependencies
pull_changes >> reload_all_dags