from airflow.sdk import dag, task
from airflow.models import Variable

@dag
def ms_clarity_pipeline():

    @task
    def fetch_data():
        import requests

        params = {
            "numOfDays": 3,
            "dimension1": "Country/Region",
            #"dimension2": "Source",
            #"dimension3": "Device",
        }
        headers = {
            "Authorization": f"Bearer {Variable.get('ms_clarity_portfolio_token')}",
            "Content-Type": "application/json",
        }
        response = requests.get(
            "https://www.clarity.ms/export-data/api/v1/project-live-insights",
            params=params,
            headers=headers
        )
        if response.status_code != 200:
            print(response.status_code)
        else:
            return response.json()

    fetch_data()
ms_clarity_pipeline()