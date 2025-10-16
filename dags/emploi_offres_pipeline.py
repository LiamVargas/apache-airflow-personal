from airflow.sdk import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowSkipException
from docker.types import Mount


def _check_appellation_gsheet_exist():
    g_drive_hook = GoogleDriveHook(
        api_version='v3',
        gcp_conn_id='gcp_sa__exerani_eop_extract'
    )
    _exists = g_drive_hook.exists(
        folder_id=Variable.get(
            'france_travail_gsuite_appellations',
            deserialize_json=True).get('gdrive_id'),
        file_name='appellations'
    )
    if _exists:
        return 'get_appellation_ids'
    else:
        raise AirflowSkipException()        


def _get_appellation_ids(ti):
    g_sheets_hook = GSheetsHook(
        gcp_conn_id='gcp_sa__exerani_eop_extract',
        api_version='v4'
    )
    ids = g_sheets_hook.get_values(
        spreadsheet_id=Variable.get(
            'france_travail_gsuite_appellations',
            deserialize_json=True).get('gsheet_id'),
        range_=Variable.get(
            'france_travail_gsuite_appellations',
            deserialize_json=True).get('range')
    )
    ids = [int(id[0]) for id in ids]

    if ids:
        ti.xcom_push(key='job_interests',value=ids)
    else:
        raise AirflowSkipException()


with DAG(
    dag_id='emploi_offres_pipeline',
    schedule='@daily',
    start_date=datetime(2025, 10, 14),
    catchup=True,
    max_active_runs=1,
) as dag:


    check_appellation_gsheets_exist = BranchPythonOperator(
        task_id='check_appellation_gsheets_exist',
        python_callable=_check_appellation_gsheet_exist
    )


    get_appellation_ids = PythonOperator(
        task_id='get_appellation_ids',
        python_callable=_get_appellation_ids
    )


    start_offres_container_or1 = DockerOperator(
        task_id = 'start_offres_container_or1',
        wait_for_downstream = True,
        api_version = 'auto',
        image = 'extract-frt-offres:latest',
        docker_url = 'unix://var/run/docker.sock',
        network_mode = 'bridge',
        auto_remove = 'success',
        mount_tmp_dir = False,
        environment = {
            'client_id': Variable.get('france_travail_client_id'),
            'client_secret': Variable.get('france_travail_client_secret'),
            'scope': Variable.get('france_travail_scope'),
            'origin': 1,
            'query_max_date': "{{ dag_run.logical_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            'query_min_date': "{{ (dag_run.logical_date - macros.dateutil.relativedelta.relativedelta(months=3)).strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            'rome_codes': '{{ ti.xcom_pull(task_ids="get_appellation_ids", key="job_interests") }}',
            'test': '{{ dag_run.logical_date }}'
        },
        mounts = [
            Mount(source='/home/liamv/python_projects/emploi_project/emploi_offres/logs', target='/emploi_offres/logs', type='bind'),
            Mount(source='/home/liamv/python_projects/emploi_project/downloads', target='/downloads/', type='bind')
        ]
    )


    start_offres_container_or2 = DockerOperator(
        task_id='start_offres_container_or2',
        wait_for_downstream=True,
        api_version='auto',
        image='extract-frt-offres:latest',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove='success',
        mount_tmp_dir=False,
        environment={
            'client_id': Variable.get('france_travail_client_id'),
            'client_secret': Variable.get('france_travail_client_secret'),
            'scope': Variable.get('france_travail_scope'),
            'origin': 2,
            'query_max_date': "{{ dag_run.logical_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            'query_min_date': "{{ (dag_run.logical_date - macros.dateutil.relativedelta.relativedelta(months=3)).strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            'rome_codes': '{{ ti.xcom_pull(task_ids="get_appellation_ids", key="job_interests") }}',
            'test': '{{ dag_run.logical_date }}'
        },
        mounts=[
            Mount(source='/home/liamv/python_projects/emploi_project/emploi_offres/logs', target='/emploi_offres/logs',type='bind'),
            Mount(source='/home/liamv/python_projects/emploi_project/downloads', target='/downloads/', type='bind')
        ],
    )


    check_appellation_gsheets_exist >> get_appellation_ids >> [start_offres_container_or1, start_offres_container_or2]
