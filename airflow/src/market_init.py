import asyncio
import html
import json
import re
import time

import asyncpg
import httpx

from src.conf.config import LOCALES, PG_CONF, PRE_DEFINED
from src.models.items import Item

credentials = PG_CONF

async def insert_items(items: dict) -> None:
    conn = await asyncpg.connect(
        user=credentials["PG_USER"],
        password=credentials["PG_PASSWORD"],
        database=credentials["PG_DB"],
        host=credentials["PG_HOST"],
        port=credentials["PG_PORT"],
    )

    # insert the DB static data
    for _item_id, item_data in PRE_DEFINED.items():
        item = Item.parse_obj(item_data)
        await conn.execute(
            "INSERT INTO codex_data.items (id, locale_default, locale_name, grade, category_primary, category_secondary) "
            "VALUES ($1, $2, $3, $4, $5, $6) "
            "ON CONFLICT (id, locale_default) DO UPDATE SET "
            "locale_name = EXCLUDED.locale_name, "
            "grade = EXCLUDED.grade, "
            "category_primary = EXCLUDED.category_primary, "
            "category_secondary = EXCLUDED.category_secondary",
            item.id,
            item.locale_default,
            json.dumps(item.locale_name),
            item.grade,
            item.category_primary,
            item.category_secondary,
        )

    # Insert the items into the table
    for _item_id, item_data in items.items():
        item = Item.parse_obj(item_data)
        await conn.execute(
            "INSERT INTO codex_data.items (id, locale_default, locale_name, grade, category_primary, category_secondary) "
            "VALUES ($1, $2, $3, $4, $5, $6) "
            "ON CONFLICT (id, locale_default) DO UPDATE SET "
            "locale_name = EXCLUDED.locale_name, "
            "grade = EXCLUDED.grade, "
            "category_primary = EXCLUDED.category_primary, "
            "category_secondary = EXCLUDED.category_secondary",
            item.id,
            item.locale_default,
            json.dumps(item.locale_name),
            item.grade,
            item.category_primary,
            item.category_secondary,
        )

    await conn.close()


async def scrape_and_insert(locale: str) -> None:
    max_retries = 3
    retry_delay = 10  # seconds
    async with httpx.AsyncClient(timeout=300) as client:
        for i in range(max_retries):
            try:
                url = f"https://bdocodex.com/query.php?a=items&l={locale}&_={int(time.time()*1000)}"
                response = await client.get(url)
                input_text = response.text.replace("\ufeff", "").replace("\\r\\n", "")
                data = json.loads(input_text)["aaData"]

                items = {}
                for item in data:
                    item_id = int(item[0])
                    item_name = html.unescape(re.sub(r"<[^>]+>", "", item[2]))
                    item_grade = int(item[5])

                    item_data = {
                        "id": item_id,
                        "locale_default": locale,
                        "locale_name": {locale: item_name},
                        "grade": item_grade,
                        "category_primary": None,  # these are updated later
                        "category_secondary": None,
                    }

                    items[item_id] = item_data

                await insert_items(items)
                return
            except (httpx.RequestError, json.JSONDecodeError):
                if i < max_retries - 1:
                    print(f"{locale} - error, retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    print(f"{locale} - error, max retries exceeded")


async def main():
    conn = await asyncpg.connect(
        user=credentials["PG_USER"],
        password=credentials["PG_PASSWORD"],
        database=credentials["PG_DB"],
        host=credentials["PG_HOST"],
        port=credentials["PG_PORT"],
    )

    # Drop the table if it already exists
    await conn.execute("DROP TABLE IF EXISTS codex_data.items")

    # Create a new table with the desired schema
    await conn.execute(
        "CREATE TABLE codex_data.items ("
        "id INTEGER, "
        "locale_default TEXT, "
        "locale_name JSONB, "
        "grade INTEGER, "
        "category_primary TEXT, "
        "category_secondary TEXT, "
        "PRIMARY KEY (id, locale_default))"
    )

    await conn.close()

    tasks = []
    for locale in LOCALES:
        task = asyncio.create_task(scrape_and_insert(locale))
        tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
