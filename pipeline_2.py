import datetime
import sqlite3

import pandas as pd
import numpy as np


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


# df_data = get_df_data()
# df_item = get_df_item()
# df_user = get_df_user()
sql_connector = SqlConnector()
sql_connector.get_mean_age_of_user()
sql_connector.get_names_of_top_rated_movies()
top_genres = sql_connector.get_top_genres()
sql_connector.conn.close()
