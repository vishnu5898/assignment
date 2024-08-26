from typing import Tuple
import datetime
import random

from bs4 import BeautifulSoup, Tag
import httpx
import numpy as np
import pandas as pd
import sqlite3

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DagRun


logger = LoggingMixin().log


TICKERS = ["HDFC", "Tata Motors"]
YOURSTORY_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Origin': 'https://yourstory.com',
    'Referer': 'https://yourstory.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'content-type': 'application/x-www-form-urlencoded',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
}
FINSHOTS_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Origin': 'https://finshots.in',
    'Referer': 'https://finshots.in/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
}


class SqlStorage:
    def __init__(self) -> None:
        self.file_name = "ingested_data"
        self.table_name = self.file_name
        self.conn = sqlite3.connect(self.file_name + ".db")
        self.create_table()

    def create_table(self) -> None:
        query = (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} "
            f"(ticker TEXT, title TEXT, text TEXT, published_at DATETIME, data_source TEXT, sentiment_score FLOAT, CONSTRAINT {self.table_name}_unique UNIQUE (title, text))"
        )
        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()

    def insert_data(self, data_dict: dict) -> None:
        columns = ', '.join(data_dict.keys())
        placeholders = ':'+', :'.join(data_dict.keys())
        query = f'INSERT INTO {self.table_name} (%s) VALUES (%s) ON CONFLICT (title, text) DO NOTHING' % (columns, placeholders)
        cur = self.conn.cursor()
        cur.execute(query, data_dict)
        self.conn.commit()


def download_response(bytes_data: bytes, file_name: str = "a.html") -> None:
    with open(file_name, "wb") as f:
        f.write(bytes_data)


def get_latest_data_for_ticker(client: httpx.Client, old_ticker: str) -> Tuple[list, list]:
    """
        Get latest 5 articles for the given ticker.
    """

    # Request for yourstory data
    ticker = old_ticker.replace(" ", "%20")
    data = (
            '{"requests":[{"indexName":"production_STORIES","params":'
            '"facets=%5B%22authors.name%22%2C%22brand.name%22%2C%22category.name%22%2C%22tags.name%22%2C%22type%22%5D&'
            'highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&'
            'hitsPerPage=20&maxValuesPerFacet=50&page=0&query=' + ticker + '&tagFilters="}]}'
    )
    response = client.post(
        'https://c68zyjx3fr-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.24.0)%3B%20Browser%20(lite)%3B%20JS%20Helper%20(3.14.0)%3B%20react%20(18.3.0-canary-60a927d04-20240113)%3B%20react-instantsearch%20(6.40.4)&x-algolia-api-key=7ca080075692fecd8959c5435957e31e&x-algolia-application-id=C68ZYJX3FR',
        headers=YOURSTORY_HEADERS,
        data=data,
    )
    # download_response(response.content, file_name="a.json")
    json_data = response.json()

    for x in json_data["results"][0]["hits"]:
        x["publishedAt"] = str(x["publishedAt"])
        try:
            x["publishedAt"] = datetime.datetime.fromtimestamp(float(f'{x["publishedAt"][:10]}.{x["publishedAt"][10:]}'))
        except Exception:
            print(x["publishedAt"])


    # Sort by latest 5 articles
    data = sorted([i for i in json_data["results"][0]["hits"]], key=lambda x: x["publishedAt"], reverse=True)

    # Get first 5 elements
    required_data_yourstory = data[:5]

    # Request for finshots data
    params = {
        'q': old_ticker,
    }

    response = client.get('https://backend.finshots.in/backend/search/', params=params, headers=FINSHOTS_HEADERS)
    # download_response(response.content, file_name="a.json")

    json_data = response.json()

    new_json_data = []
    for x in json_data["matches"]:
        try:
            x["published_date"] = datetime.datetime.strptime(x["published_date"], "%Y-%m-%dT%H:%M:%S.%f+00:00")
        except Exception:
            try:
                x["published_date"] = datetime.datetime.strptime(x["published_date"], "%Y-%m-%dT%H:%M:%S+00:00")
            except:
                print(x["published_date"])
                breakpoint()
        new_json_data.append(x)
        

    # Sort by latest 5 articles
    data = sorted([i for i in json_data["matches"]], reverse=True, key=lambda x: x["published_date"])
    # Get first 5 elements
    required_data_finshots = data[:5]

    return required_data_yourstory, required_data_finshots


def extract_title_and_text_from_yourstory(data: list):
    new_data = []
    for data_dict in data:
        new_data_dict = {}
        new_data_dict["title"] = data_dict["title"].strip().title()
        new_data_dict["text"] = data_dict["postContent"].strip()
        new_data_dict["published_at"] = data_dict["publishedAt"]
        new_data_dict["data_source"] = "yourstory"

        # Add only if combination of title and text is unique
        if new_data_dict not in new_data:
            new_data.append(new_data_dict)

    return new_data


def extract_text_from_finshots_page(bytes_data: bytes) -> str:
    bs4_obj = BeautifulSoup(bytes_data, "html.parser", from_encoding="utf-8")
    div_tag = bs4_obj.find("div", attrs={"class": "post-content"})
    if not isinstance(div_tag, Tag):
        raise Exception("div_tag is not an instance of Tag element")
    return div_tag.text


def extract_title_and_text_from_finshots(client: httpx.Client, data: list):
    new_data = []
    for data_dict in data:
        new_data_dict = {}
        new_data_dict["title"] = data_dict["title"].strip().title()
        url = data_dict["post_url"]
        response = client.get(url)
        text = extract_text_from_finshots_page(response.content)
        text = text.strip()
        new_data_dict["text"] = text
        new_data_dict["published_at"] = data_dict["published_date"]
        new_data_dict["data_source"] = "finshots"

        # Add only if combination of title and text is unique
        if new_data_dict not in new_data:
            new_data.append(new_data_dict)

        # time.sleep(1)

    return new_data


def get_sentiment_score(text: str):
    return random.random()


def pipeline_1():
    client = httpx.Client()
    sql_storage = SqlStorage()

    try:
        for ticker in TICKERS:
            
            # Fetching data
            yourstory, finshots = get_latest_data_for_ticker(client, ticker)

            # Data Preparation and cleaning
            yourstory = extract_title_and_text_from_yourstory(yourstory)
            finshots = extract_title_and_text_from_finshots(client, finshots)

            final_data = sorted([*yourstory, *finshots], reverse=True, key=lambda x: x["published_at"])
            final_data = final_data[:5]
            for data in final_data:

                # Mock api
                data["sentiment_score"] = get_sentiment_score(data["text"])
                data["ticker"] = ticker
                sql_storage.insert_data(data)
    finally:
        client.close()
        sql_storage.conn.close()


DATA_COLUMNS = {
    "user_id": "INTEGER",
    "item_id": "INTEGER",
    "rating": "INTEGER",
    "timestamp": "TIMESTAMP",
}
ITEM_COLUMNS = {
    "movie_id": "INTEGER",
    "movie_title": "TEXT",
    "release_date": "TIMESTAMP",
    "video_release_date": "TIMESTAMP",
    "IMDbURL": "TEXT",
    "unknown": "INTEGER",
    "action": "INTEGER",
    "adventure": "INTEGER",
    "animation": "INTEGER",
    "childrens": "INTEGER",
    "comedy": "INTEGER",
    "crime": "INTEGER",
    "documentary": "INTEGER",
    "drama": "INTEGER",
    "fantasy": "INTEGER",
    "film_noir": "INTEGER",
    "horror": "INTEGER",
    "musical": "INTEGER",
    "mystery": "INTEGER",
    "romance": "INTEGER",
    "sci_fi": "INTEGER",
    "thriller": "INTEGER",
    "war": "INTEGER",
    "western": "INTEGER",
}

USER_COLUMNS = {
    "user_id": "INTEGER",
    "age": "INTEGER",
    "gender": "TEXT",
    "occupation": "TEXT",
    "zipcode": "TEXT",
}


class SqlConnector:
    def __init__(self) -> None:
        self.file_name = "pipeline_2"
        self.data_table = "data"
        self.item_table = "item"
        self.user_table = "user"
        self.conn = sqlite3.connect(self.file_name + ".db")
        self.conn.row_factory = sqlite3.Row
        self.create_tables()

    def create_tables(self) -> None:
        table_tuples = [
            (DATA_COLUMNS, self.data_table, "user_id, item_id"),
            (ITEM_COLUMNS, self.item_table, "movie_id"),
            (USER_COLUMNS, self.user_table, "user_id"),
        ]
        for col_types_dict, table_name, unique_cols in table_tuples:
            create_joined_stmts = ", ".join(f"{col} {col_type}" for col, col_type in col_types_dict.items())
            query = (
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"({create_joined_stmts}, CONSTRAINT {table_name}_unique UNIQUE({unique_cols}))"
            )
            cur = self.conn.cursor()
            cur.execute(query)
            self.conn.commit()

    def insert_data_to_three_tables(self) -> None:
        table_tuples = [
            (self.data_table, "user_id, item_id"),
            (self.item_table, "movie_id"),
            (self.user_table, "user_id"),
        ]
        for table_name, unique_cols in table_tuples:
            pass
            

    def insert_data(self, data_dict: dict, table_name: str, unique_cols: str) -> None:
        columns = ', '.join(data_dict.keys())
        placeholders = ':'+', :'.join(data_dict.keys())
        query = f'INSERT INTO {table_name} (%s) VALUES (%s) ON CONFLICT ({unique_cols}) DO NOTHING' % (columns, placeholders)
        cur = self.conn.cursor()
        cur.execute(query, data_dict)

    def get_mean_age_of_user(self):
        query = (
            f"SELECT occupation, AVG(age) as mean_age FROM user GROUP BY 1 ORDER BY 1;"
        )
        cur = self.conn.cursor()
        cur.execute(query)
        rows = [dict(i) for i in cur.fetchall()]
        return rows

    def get_names_of_top_rated_movies(self):
        query = (
            f"SELECT t2.movie_title FROM (SELECT item_id, COUNT(user_id) AS no_of_ratings "
            f"FROM {self.data_table} GROUP BY 1 HAVING COUNT(user_id) >= 35 ORDER BY 2 DESC LIMIT 20) "
            f"AS t1 INNER JOIN {self.item_table} AS t2 ON t1.item_id = t2.movie_id ORDER BY t1.no_of_ratings DESC;"
        )
        cur = self.conn.cursor()
        cur.execute(query)
        rows = [dict(i) for i in cur.fetchall()]
        return rows

    def get_age_group(self) -> list:
        query = (
            "SELECT *, CASE WHEN ((20 <= age) AND (age <= 25)) THEN '20-25' "
            "WHEN ((26 <= age) AND (age <= 35)) THEN '26-35' "
            "WHEN ((36 <= age) AND (age <= 45)) THEN '36-45' ELSE 'other' END "
            f"AS age_group FROM {self.user_table}"
        )
        cur = self.conn.cursor()
        cur.execute(query)
        rows = [dict(i) for i in cur.fetchall()]
        return rows

    def get_item_rows(self) -> list:
        query = (
            f"SELECT * FROM {self.item_table}"
        )
        cur = self.conn.cursor()
        cur.execute(query)
        rows = [dict(i) for i in cur.fetchall()]
        return rows

    def get_data_rows(self) -> list:
        query = (
            f"SELECT * FROM {self.data_table}"
        )
        cur = self.conn.cursor()
        cur.execute(query)
        rows = [dict(i) for i in cur.fetchall()]
        return rows

    def get_top_genres(self) -> list:
        age_group_rows = self.get_age_group()
        item_rows = self.get_item_rows()
        modified_rows = []
        for row in item_rows:
            row["genre"] = []
            for col in ITEM_COLUMNS:
                if col not in [
                    "movie_id",
                    "movie_title",
                    "release_date",
                    "video_release_date",
                    "IMDbURL",
                ]:
                    if row[col] == 1:
                        row["genre"].append(col)
            modified_rows.append(row)
        data_rows = self.get_data_rows()
        df_age = pd.DataFrame(age_group_rows)
        df_item = pd.DataFrame(modified_rows)
        df_data = pd.DataFrame(data_rows)
        df_data = df_data.rename(columns={"item_id": "movie_id"})
        merged_df = pd.merge(df_data, df_item, how="inner", on=["movie_id"])
        merged_df = pd.merge(merged_df, df_age, how="inner", on=["user_id"])
        merged_df = merged_df.explode("genre").reset_index(drop=True)
        merged_df = merged_df.groupby(["occupation", "age_group", "genre"])["user_id"].nunique().reset_index()
        merged_df = merged_df.sort_values(by=["occupation", "age_group", "user_id"], ascending=[True, True, False])
        merged_df = merged_df.groupby(["occupation", "age_group"]).head(5)
        merged_df = merged_df.groupby(["occupation", "age_group"])["genre"].aggregate([list]).reset_index()
        merged_df = merged_df.rename(columns={"list": "genre"})
        self.conn.close()
        return merged_df[["occupation", "age_group", "genre"]].to_dict("records")
        

def convert_types_of_df_data(row: pd.Series) -> pd.Series:
    data_dict = row.to_dict()

    # Converting to integer
    for key in data_dict:
        data_dict[key] = int(data_dict[key])
    data_dict["timestamp"] = datetime.datetime.fromtimestamp(data_dict["timestamp"]).timestamp()
    row = pd.Series(data_dict)
    return row


def convert_types_of_df_item(row: pd.Series) -> pd.Series:
    data_dict = row.to_dict()

    # Converting to integer
    for key in [
        "movie_id",
        "unknown",
        "action",
        "adventure",
        "animation",
        "childrens",
        "comedy",
        "crime",
        "documentary",
        "drama",
        "fantasy",
        "film_noir",
        "horror",
        "musical",
        "mystery",
        "romance",
        "sci_fi",
        "thriller",
        "war",
        "western",
    ]:
        data_dict[key] = int(data_dict[key])
    if data_dict["release_date"] != "":
        data_dict["release_date"] = datetime.datetime.strptime(data_dict["release_date"], "%d-%b-%Y").timestamp()
    if data_dict["video_release_date"] != "":
        data_dict["video_release_date"] = datetime.datetime.strptime(data_dict["video_release_date"], "%d-%b-%Y").timestamp()
    row = pd.Series(data_dict)
    return row


def convert_types_of_df_user(row: pd.Series) -> pd.Series:
    data_dict = row.to_dict()

    # Converting to integer
    for key in [
        "user_id",
        "age",
    ]:
        data_dict[key] = int(data_dict[key])

    row = pd.Series(data_dict)
    return row


def get_df_data():
    df_data = pd.read_csv("u.data", dtype='str', delimiter="\t", header=None, encoding="ISO-8859-1")
    df_data.columns = list(DATA_COLUMNS)
    df_data = df_data.replace(np.nan, "")
    df_data = df_data.apply(convert_types_of_df_data, axis=1)
    return df_data


def get_df_item():
    df_item = pd.read_csv("u.item", dtype='str', delimiter="|", header=None, encoding="ISO-8859-1")
    df_item.columns = list(ITEM_COLUMNS)
    df_item = df_item.replace(np.nan, "")
    df_item = df_item.apply(convert_types_of_df_item, axis=1)
    return df_item


def get_df_user():
    df_user = pd.read_csv("u.user", dtype='str', delimiter="|", header=None, encoding="ISO-8859-1")
    df_user.columns = list(USER_COLUMNS)
    df_user = df_user.replace(np.nan, "")
    df_user = df_user.apply(convert_types_of_df_user, axis=1)
    return df_user



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2024, 8, 26),
}


# Pipeline 1
dag_1 = DAG(
    'pipeline_1',
    default_args=default_args,
    description="Pipeline 1",
    schedule="19 00 * * *"
)
etl_1 = PythonOperator(
    task_id="data_ingestion",
    python_callable=pipeline_1,
    dag=dag_1
)


# Pipeline 2
dag_2 = DAG(
    'pipeline_2',
    default_args=default_args,
    description="Pipeline 2",
    schedule="20 00 * * *"
)




def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="pipeline_1")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date


wait_for_external_dag = ExternalTaskSensor(
        execution_date_fn=get_most_recent_dag_run,
        task_id='wait_for_external_dag',
        external_dag_id='pipeline_1',  # The DAG ID of the external DAG
        mode="reschedule",
        allowed_states=['success'],       # States to consider as successful
        failed_states=['failed',],  # States to consider as failed
        dag=dag_2,
        timeout=60
    )
sql_connector = SqlConnector()
mean_age = PythonOperator(
    task_id="mean_age",
    python_callable=sql_connector.get_mean_age_of_user,
    dag=dag_2
)
top_rated_movies = PythonOperator(
    task_id="top_rated_movies",
    python_callable=sql_connector.get_names_of_top_rated_movies,
    dag=dag_2
)
top_genres = PythonOperator(
    task_id="top_genres",
    python_callable=sql_connector.get_top_genres,
    dag=dag_2
)

# Task dependencies
etl_1
mean_age >> top_rated_movies >> top_genres

