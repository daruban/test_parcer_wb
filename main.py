import requests
import pandas as pd
import time
import aiohttp
import asyncio
from typing import Optional, Dict, Any
import logging
from tqdm import tqdm
import time


LIMIT_PAGE = 1
LIMIT_ITEMS = 10
QUERY = "пальто из натуральной шерсти"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("logger")

headers = {
  'accept': '*/*',
  'accept-language': 'ru,en;q=0.9',
  'deviceid': 'site_be6e8f23f58547e382e6d2b76e5530fb',
  'priority': 'u=1, i',
  'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "YaBrowser";v="26.3", "Yowser";v="2.5"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-origin',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 YaBrowser/26.3.0.0 Safari/537.36',
  'x-queryid': 'qid765694092174628686420260330141435',
  'x-requested-with': 'XMLHttpRequest',
  'x-spa-version': '14.3.2',
  'x-userid': '0'
}


async def get_card( product: Dict[str, Any], max_basket: int = 60, timeout: float = 10.0) -> tuple[Any, int] | None:
    """Export card from wildberries"""
    nm_id = product.get("id")
    vol = nm_id // 100000
    part = nm_id // 1000

    urls = [
        f"https://basket-{basket:02d}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/info/ru/card.json"
        for basket in range(1, max_basket + 1)
    ]
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        tasks = [session.get(url, headers=headers) for url in urls]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for i, resp in enumerate(responses):
            basket = i + 1
            if isinstance(resp, Exception):
                logger.error(f"basket={basket:02d} | {nm_id} → {type(resp).__name__}")
                continue
            try:
                if resp.status == 200:
                    data = await resp.json()
                    logger.debug(f"basket={basket:02d} → nm={nm_id}")
                    return {"product" : product, "card_data": data, "basket": basket}
                else:
                    pass
                    logger.debug(f"basket={basket:02d} | status={resp.status} | nm={nm_id}")
            except Exception as e:
                logger.error(f"basket={basket:02d} | json parse error → {e}")
            finally:
                if not resp.closed:
                    await resp.release()


async def export_products_and_cards(query, sleep_sec=0.5):
    """Export items from wildberries"""
    search_url = "https://search.wb.ru/exactmatch/ru/common/v18/search"
    params = {
        "appType": 1,
        "curr": "rub",
        "dest": -1257786,
        "lang": "ru",
        "page": 1,
        "query": query,
        "resultset": "catalog",
        "sort": "popular",
        "spp": 100
    }
    products = []
    logger.info("Выполняем поиск товаров...")


    def get_products():
        response = requests.get(search_url, params=params, headers=headers)
        logger.debug(f"Поиск: статус {response.status_code}")
        while response.status_code != 200:
            logger.error(f"Ошибка поиска: {response.status_code}")
            response = requests.get(search_url, params=params, headers=headers)
            time.sleep(sleep_sec)
        return response.json()


    data = get_products()
    logger.info(f'total: {data["total"]}')
    for i in range(LIMIT_PAGE):
        params['page'] = i + 1
        products.extend(get_products()["products"])

    logger.info(f"Найдено товаров: {len(products)}")

    items = []
    for product in tqdm(products[0:LIMIT_ITEMS]):
        item_data = await get_card(product)
        if item_data:
            items.append(item_data)
        time.sleep(sleep_sec)
    return items


async def pars(items):
    """parser"""
    pars_items = []
    logger.info(f"parser")
    for item in tqdm(items):
        product = item["product"]
        card_data = item["card_data"]
        basket= item["basket"]
        nm_id = product.get("id")

        pars_item = {
            "Ссылка на товар":          f"https://www.wildberries.ru/catalog/{card_data['nm_id']}/detail.aspx",
            "Артикул WB":               card_data['nm_id'],
            "Название":                 card_data['imt_name'],
            "Цена":                     product.get("sizes", [{}])[0].get("price", {}).get("product", 0)/100,
            "Описание":                 card_data.get("description"),
            "Ссылки на изображения":    ",".join([f"https://basket-{basket:02d}.wbbasket.ru/vol{nm_id // 100000}/part{nm_id // 1000}/{card_data['nm_id']}/images/big/{i}.webp" for i in range(1, card_data.get("media", 0).get("photo_count", 0)+1)]),
            "Характеристики":           str([{"name": opt.get("name"), "value": opt.get("value")} for opt in card_data.get("options", [])]),
            "Название селлера":         product.get("supplier"),
            "Ссылка на селлера":        f"https://www.wildberries.ru/seller/{product.get('supplier')}",
            "Размеры":                  ",".join([s.get("origName") for s in product.get("sizes", []) if s.get("origName")]),
            "Остатки":                  product.get("totalQuantity", 0),
            "Рейтинг":                  product.get("reviewRating", 0),
            "Отзывы":                   product.get("feedbacks", 0)
        }

        pars_items.append(pars_item)
    return pars_items


async def import_xlsx(items):
    """импорт в xlsx"""
    df = pd.DataFrame(items)
    df.to_excel("full_catalog.xlsx", index=False)

    df_filtered = df[
        (df["Рейтинг"] >= 4.5) &
        (df["Цена"] <= 10000) &
        (df["Характеристики"].str.contains("{'name': 'Страна производства', 'value': 'Россия'}"))
    ].copy()

    df_filtered.to_excel("filtered_catalog.xlsx", index=False)
    df.to_excel("full_catalog.xlsx", index=False)
    print("[INFO] Сохранено wb_products.csv")


async def main():
    items = await export_products_and_cards(QUERY)
    pars_items = await pars(items)
    await import_xlsx(pars_items)


if __name__ == "__main__":
    asyncio.run(main())