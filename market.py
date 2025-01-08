import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Fetches a list of product offer mapping entries from Yandex.Market API.

    This function retrieves a paginated list of product offers associated with a specific campaign
    using the provided access token for authorization.

    Args:
        page (str): The token for the page of results to retrieve.
        campaign_id (str): The ID of the campaign to fetch offers for.
        access_token (str): The OAuth2 access token for authenticating the API request.

    Returns:
        list: A list of product offer mapping entries if the request is successful.

    Example of correct usage:
        product_list = get_product_list("page_token_example", "123456", "your_access_token")

    Example of incorrect usage:
        product_list = get_product_list("invalid_page_token", "invalid_campaign_id", "invalid_access_token")
        # This will raise an HTTPError if the request fails due to invalid parameters.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Updates the stock information for the specified products in the Yandex Market API.

    This function sends a PUT request to the Yandex Market API to update the stock levels
    for the given list of stock keeping units (SKUs) associated with a specific campaign.

    Args:
        stocks (list): A list of stock keeping units (SKUs) to be updated.
        campaign_id (str): The ID of the campaign for which the stocks are being updated.
        access_token (str): The access token used for authentication with the Yandex Market API.

    Returns:
        dict: A JSON response object containing the result of the stock update operation.

    Example of correct usage:
        response = update_stocks(['sku1', 'sku2', 'sku3'], 'campaign_123', 'your_access_token')

    Example of incorrect usage:
        response = update_stocks([], 'invalid_campaign_id', 'invalid_token')
        # This will raise an error as an empty list of stocks cannot be processed.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Updates the stock levels of products in Yandex.Market API.

    This function sends a request to update the stock levels for a list of products
    associated with a specific campaign using the provided access token for authorization.

    Args:
        stocks (list): A list of stock keeping units (SKUs) with their updated stock levels.
        campaign_id (str): The ID of the campaign to update stocks for.
        access_token (str): The OAuth2 access token for authenticating the API request.

    Returns:
        dict: A response object containing the result of the stock update request if successful.

    Example of correct usage:
        response = update_stocks([{"sku": "sku_example_1", "stock": 10}, {"sku": "sku_example_2", "stock": 5}], "123456", "your_access_token")

    Example of incorrect usage:
        response = update_stocks([{"sku": "invalid_sku", "stock": -1}], "invalid_campaign_id", "invalid_access_token")
        # This will raise an HTTPError if the request fails due to invalid parameters.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Retrieves the offer IDs of products from Yandex.Market API.

    This function fetches all product offer mapping entries associated with a specific campaign
    using the provided market token for authorization. It handles pagination to ensure all offers
    are retrieved.

    Args:
        campaign_id (str): The ID of the campaign to fetch offer IDs for.
        market_token (str): The OAuth2 access token for authenticating the API request.

    Returns:
        list: A list of offer IDs (shop SKUs) for the products associated with the specified campaign.

    Example of correct usage:
        offer_ids = get_offer_ids("123456", "your_market_token")

    Example of incorrect usage:
        offer_ids = get_offer_ids("invalid_campaign_id", "invalid_market_token")
        # This will raise an HTTPError if the request fails due to invalid parameters.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Creates a list of stock entries for products based on the provided watch remnants and offer IDs.

    This function processes the watch remnants to generate stock information for products that are
    available in the market. It checks if the product codes from the watch remnants exist in the
    provided offer IDs and constructs stock entries accordingly. If a product is not found in the
    watch remnants, it adds an entry with a stock count of zero.

    Args:
        watch_remnants (list): A list of dictionaries containing product information, including
                                product codes and quantities.
        offer_ids (list): A list of offer IDs (product codes) that are currently available in the market.
        warehouse_id (str): The ID of the warehouse where the stock is held.

    Returns:
        list: A list of stock entries formatted for the API, each containing SKU, warehouse ID,
                and stock count information.

    Example of correct usage:
        stocks = create_stocks(watch_remnants_data, ["sku_example_1", "sku_example_2"], "warehouse_123")

    Example of incorrect usage:
        stocks = create_stocks([], [], "invalid_warehouse_id")
        # This will return an empty list as there are no watch remnants or offer IDs to process.
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Creates a list of price entries for products based on the provided watch remnants and offer IDs.

    This function processes the watch remnants to generate price information for products that are
    available in the market. It checks if the product codes from the watch remnants exist in the
    provided offer IDs and constructs price entries accordingly.

    Args:
        watch_remnants (list): A list of dictionaries containing product information, including
                                product codes and prices.
        offer_ids (list): A list of offer IDs (product codes) that are currently available in the market.

    Returns:
        list: A list of price entries formatted for the API, each containing product ID and price information.

    Example of correct usage:
        prices = create_prices(watch_remnants_data, ["sku_example_1", "sku_example_2"])

    Example of incorrect usage:
        prices = create_prices([], [])
        # This will return an empty list as there are no watch remnants or offer IDs to process.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Uploads price entries for products based on the provided watch remnants, campaign ID, and market token.

    This asynchronous function retrieves the offer IDs associated with the specified campaign and
    generates price information for products available in the market using the `create_prices` function.
    It then divides the price entries into batches of 500 and updates the prices in the market using
    the `update_price` function for each batch.

    Args:
        watch_remnants (list): A list of dictionaries containing product information, including
                                product codes and prices.
        campaign_id (str): The ID of the campaign for which the prices are being uploaded.
        market_token (str): The token used for authentication with the market API.

    Returns:
        list: A list of price entries that were uploaded, formatted for the API.

    Example of correct usage:
        uploaded_prices = await upload_prices(watch_remnants_data, "campaign_123", "market_token_abc")

    Example of incorrect usage:
        uploaded_prices = await upload_prices([], "invalid_campaign_id", "invalid_token")
        # This will return an empty list as there are no watch remnants to process.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Uploads stock entries for products based on the provided watch remnants, campaign ID, market token, and warehouse ID.

    This asynchronous function retrieves the offer IDs associated with the specified campaign and generates stock
    information for products available in the market using the `create_stocks` function. It then divides the stock
    entries into batches of 2000 and updates the stocks in the market using the `update_stocks` function for each
    batch. Finally, it filters out the stocks that have a count of zero and returns both the non-empty stocks and
    the complete list of stocks.

    Args:
        watch_remnants (list): A list of dictionaries containing product information, including product codes.
        campaign_id (str): The ID of the campaign for which the stocks are being uploaded.
        market_token (str): The token used for authentication with the market API.
        warehouse_id (str): The ID of the warehouse where the stocks are located.

    Returns:
        tuple: A tuple containing:
            - list: A list of non-empty stock entries that were uploaded, formatted for the API.
            - list: A complete list of stock entries that were created.

    Example of correct usage:
        non_empty_stocks, all_stocks = await upload_stocks(watch_remnants_data, "campaign_123", "market_token_abc", "warehouse_456")

    Example of incorrect usage:
        non_empty_stocks, all_stocks = await upload_stocks([], "invalid_campaign_id", "invalid_token", "invalid_warehouse_id")
        # This will return empty lists as there are no watch remnants to process.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
