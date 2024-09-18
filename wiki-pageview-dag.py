import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests

dag = DAG(
    dag_id="wiki-pageview",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

def _get_data(execution_date, output_path):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour-1:0>2}0000.gz"
    )
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        print("Download completed successfully.")
    else:
        print(f"Failed to download. Status code: {response.status_code}")


def _fetch_pageviews(pagenames):
    result = dict.fromkeys(pagenames, 0)
    with open("/home/zak/Development/ZakLab/my-dags/output/wiki-pageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    print(result)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "output_path": "/home/zak/Development/ZakLab/my-dags/output/wiki-pageviews.gz"
    },
    dag=dag
)


extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /home/zak/Development/ZakLab/my-dags/output/wiki-pageviews.gz",
    dag=dag
)
