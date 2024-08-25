from typing import Tuple
import datetime
import sqlite3
import time

from bs4 import BeautifulSoup, Tag
import httpx


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
            f"(title TEXT, text TEXT, published_at DATETIME, data_source TEXT, CONSTRAINT {self.table_name}_unique UNIQUE (title, text))"
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


client = httpx.Client()
sql_storage = SqlStorage()

try:
    for ticker in TICKERS:
        yourstory, finshots = get_latest_data_for_ticker(client, ticker)
        yourstory = extract_title_and_text_from_yourstory(yourstory)
        finshots = extract_title_and_text_from_finshots(client, finshots)

        final_data = sorted([*yourstory, *finshots], reverse=True, key=lambda x: x["published_at"])
        final_data = final_data[:5]
        for data in final_data:
            sql_storage.insert_data(data)
finally:
    client.close()
    sql_storage.conn.close()
