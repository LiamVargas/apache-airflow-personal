from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.docker.operators.docker import DockerOperator


def _check_appellation_gsheet_exist():
    g_drive_hook = GoogleDriveHook(
        api_version='v3',
        gcp_conn_id='gcp_conn_emploi'
    )
    _exists = g_drive_hook.exists(
        folder_id='1auRYolVSD-crY3-v_TYbE2Q_2E9NZTpX',
        file_name='appellations'
    )
    if _exists:
        print("Existe")
    else:
        print("No existe")


def _get_appellation_ids(ti):
    g_sheets_hook = GSheetsHook(
        gcp_conn_id='gcp_conn_emploi',
        api_version='v4'
    )
    ids = g_sheets_hook.get_values(
        spreadsheet_id='1NFOLLrvh63BWqoEvSQmGovzmJZhWpm2sqhANhqCsBcI', # variable en airflow
        range_="'interests'!A2:A28"
    )
    ids = [int(id[0]) for id in ids]
    ti.xcom_push(key='job_interests',value=ids)
    return ids
 


with DAG(
    dag_id='emploi_offres_pipeline',
    schedule='@daily',
    start_date=datetime(2024,10,1),
    catchup=False) as dag:

    check_appellation_gsheets_exist = PythonOperator(
        task_id='check_appellation_gsheets_exist',
        python_callable=_check_appellation_gsheet_exist
    )

    get_appellation_ids = PythonOperator(
        task_id='get_appellation_ids',
        python_callable=_get_appellation_ids
    )

    start_offres_container = DockerOperator(
        task_id = 'start_offres_container',
        depends_on_past = True,
        wait_for_downstream = True,
        api_version = 'auto',
        image = '', #ToDo
        docker_url = 'unix://var/run/docker.sock',
        network_mode = 'bridge',
        auto_remove = 'success',
        mount_tmp_dir = False,
        environment = {} #ToDo: Lista de intereses
    )

    check_appellation_gsheets_exist >> get_appellation_ids # >> start_offres_container


