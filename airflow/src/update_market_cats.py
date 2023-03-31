import psycopg2
import requests
from psycopg2 import pool

from src.conf.config import MARKET_CATEGORIES, PG_CONF


def update_database(
    response: list[dict],
    category_primary: str,
    category_secondary: str,
    connection: psycopg2.extensions.connection,
) -> None:
    """
    Update the database with response data, and the primary and secondary categories.

    Parameters
    ----------
    response : List[Dict]
        A list of dictionaries containing the response data from the API.
    category_primary : str
        The primary category for items in the response data.
    category_secondary : str
        The secondary category for items in the response data.
    connection : psycopg2.extensions.connection
        The database connection object.
    """
    for item in response:
        item_id = item["id"]

        query = "SELECT * FROM codex_data.items WHERE id = %s"
        cursor = connection.cursor()
        cursor.execute(query, (item_id,))

        result = cursor.fetchone()

        if result:
            query = "UPDATE codex_data.items SET category_primary = %s, category_secondary = %s WHERE id = %s"
            values = (category_primary, category_secondary, item_id)
            cursor.execute(query, values)

    connection.commit()
    cursor.close()


def loop_through_categories(
    market_categories: dict[str, dict], connection_pool: pool.SimpleConnectionPool
) -> None:
    """
    Loop through market categories and subcategories, updating the database.

    Parameters
    ----------
    market_categories : Dict[str, Dict]
        A dictionary of market categories and their subcategories.
    connection_pool : pool.SimpleConnectionPool
        The connection pool for the database.
    """
    for category in market_categories:
        category_name = market_categories[category]["name"]

        for subcategory in market_categories[category]["sub_categories"]:
            subcategory_name = market_categories[category]["sub_categories"][
                subcategory
            ]["name"]

            url = f"https://api.arsha.io/v2/na/GetWorldMarketList?mainCategory={category}&subCategory={subcategory}"

            response = requests.get(url, timeout=90)
            response_data = response.json()

            connection = connection_pool.getconn()
            update_database(response_data, category_name, subcategory_name, connection)
            connection_pool.putconn(connection)


def main() -> None:
    """
    Main function for the script.
    """
    credentials = PG_CONF

    connection_pool = psycopg2.pool.SimpleConnectionPool(
        1,
        10,
        user=credentials["PG_USER"],
        password=credentials["PG_PASSWORD"],
        database=credentials["PG_DB"],
        host=credentials["PG_HOST"],
        port=credentials["PG_PORT"],
    )

    loop_through_categories(MARKET_CATEGORIES, connection_pool)

    connection_pool.closeall()


if __name__ == "__main__":
    main()
