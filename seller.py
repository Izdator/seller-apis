import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Get the list of products from the Ozon store.

    Sends a request to the Ozon API to retrieve a list of products from the store.
    It uses the provided last product ID, client ID, and seller token for authentication and pagination.

    Args:
        last_id (str): The ID of the last product. Used for pagination.
        client_id (str): The unique client ID provided by Ozon.
        seller_token (str): The seller's authorization token required to make API requests.

    Returns:
        list: A list of products in the store, obtained from the API response.
              The returned list may be empty if there are no products.

    Example:
        >>> get_product_list("12345", "your_client_id", "your_seller_token")
        [{'id': 1, 'name': 'Product 1'}, {'id': 2, 'name': 'Product 2'}]

    Raises:
        requests.exceptions.HTTPError: If the API request fails.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Retrieve the offer IDs of products from the Ozon store.

    This function calls the `get_product_list` function in a loop to
    obtain all products available in the store. It handles pagination
    by using the last_product_id obtained in each response. The
    retrieved products' offer IDs are then extracted and returned as a list.

    Args:
        client_id (str): The unique client ID provided by Ozon.
        seller_token (str): The seller's authorization token required
                            to make API requests.

    Returns:
        list: A list of offer IDs for all products in the store. The
              returned list may be empty if there are no products.

    Example:
        >>> get_offer_ids("your_client_id", "your_seller_token")
        ['offer_id_1', 'offer_id_2', ...]

    Raises:
        Exception: Raises an exception if there is an error during
                   product retrieval (e.g., API request fails).
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Update product prices in the Ozon marketplace.

    This function sends a request to update the prices of products provided in a list.
    It constructs an HTTP POST request to the Ozon API with the necessary headers
    and a payload containing the new prices. If the operation is successful,
    the function returns the API response as a dictionary.

    Args:
        prices (list): A list of product prices to be updated. Each element in the list
                       should be a dictionary containing the product identifier and the
                       new price. Example:
                       [{"offer_id": "offer_id_1", "price": 1000},
                        {"offer_id": "offer_id_2", "price": 1500}].
        client_id (str): A unique identifier for the client provided by Ozon.
        seller_token (str): A seller authorization token required to make API requests.

    Returns:
        dict: The response from the Ozon API, containing the result of the price update
              operation. It may include status information and potential errors.

    Raises:
        requests.exceptions.HTTPError: Raises an exception if the API returns an error,
                                        for example, in case of incorrect data or
                                        authorization issues.

    Example:
        >>> update_price([
            {"offer_id": "offer_id_1", "price": 1000},
            {"offer_id": "offer_id_2", "price": 1500}
        ], "your_client_id", "your_seller_token")
        {'status': 'success', 'updated_count': 2}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Update product stocks in the Ozon marketplace.

    This function sends a request to update the stock quantities of products provided in a list.
    It constructs an HTTP POST request to the Ozon API with the necessary headers and a payload
    containing the new stock information. If the operation is successful, the function returns
    the API response as a dictionary.

    Args:
        stocks (list): A list of stock quantities to be updated. Each element in the list should
                       be a dictionary containing the product identifier and the updated stock
                       level. Example:
                       [{"offer_id": "offer_id_1", "stock": 50},
                        {"offer_id": "offer_id_2", "stock": 100}].
        client_id (str): A unique identifier for the client provided by Ozon.
        seller_token (str): A seller authorization token required to make API requests.

    Returns:
        dict: The response from the Ozon API, containing the result of the stock update operation.
              It may include status information and potential errors.

    Raises:
        requests.exceptions.HTTPError: Raises an exception if the API returns an error,
                                        for instance, in case of incorrect data or authorization issues.

    Example:
        >>> update_stocks([
        >>>     {"offer_id": "offer_id_1", "stock": 50},
        >>>     {"offer_id": "offer_id_2", "stock": 100}
        >>> ], "your_client_id", "your_seller_token")
        {'status': 'success', 'updated_count': 2}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Download the stock file from the Casio website.

    This function downloads a zip archive containing stock data for watches from the specified URL.
    After downloading, it unzips the archive and extracts the Excel file containing stock data.
    The function converts the contents of the Excel file into a list of dictionaries representing the watch stocks,
    and returns this list. The Excel file is deleted after processing to save space.

    Returns:
        list: A list of dictionaries, each containing data about the watch stocks.
              The data is loaded from the file "ostatki.xls", which is downloaded from the
              Casio website and processed using the pandas library.

    Raises:
        requests.exceptions.HTTPError: Raised if the request to download the file fails.
        FileNotFoundError: Raised if the file "ostatki.xls" is not found after extraction.

    Example:
        >>> watch_remnants = download_stock()
        >>> print(watch_remnants[0])
        {'model': 'Model 1', 'stock': 10, ... }
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Create a list of stock information for given offer IDs based on available watch remnants.

    This function processes a list of watch remnants, which are dictionaries containing stock information,
    and generates a list of stock dictionaries for the specified offer IDs. It checks which
    watch codes (offer IDs) are available, determines the stock count for each, and fills in
    any missing offer IDs with a stock of 0.

    Args:
        watch_remnants (list): A list of dictionaries, each representing the stock data of a watch,
            with keys such as "Код" (code) and "Количество" (quantity).
        offer_ids (set): A set of strings representing the offer IDs for which stock data is requested.

    Returns:
        list: A list of dictionaries, where each dictionary represents an offer ID and its associated stock.
            Each dictionary contains the keys "offer_id" and "stock".

    Example:
        >>> watch_remnants = [{'Код': '123', 'Количество': '5'}, {'Код': '456', 'Количество': '>10'}]
        >>> offer_ids = {'123', '456', '789'}
        >>> stocks = create_stocks(watch_remnants, offer_ids)
        >>> print(stocks)
        [{'offer_id': '123', 'stock': 5}, {'offer_id': '456', 'stock': 100}, {'offer_id': '789', 'stock': 0}]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Creates a list of prices for specified offer IDs based on available watch remnants.

    This function processes a list of watch remnants, which are dictionaries containing price information.
    It generates a list of dictionaries containing prices for the specified offer IDs. The function checks
    which watch codes (offer IDs) are available and creates a price dictionary for each of them.

    Arguments:
        watch_remnants (list): A list of dictionaries, each representing data about watches,
        with keys such as "Code" and "Price".

        offer_ids (set): A set of strings representing offer IDs for which price information is requested.

    Returns:
        list: A list of dictionaries, where each dictionary represents an offer ID and its associated price.
        Each dictionary contains the keys "offer_id", "price", "old_price", "currency_code", and "auto_action_enabled".

    Example:
        >>> watch_remnants = [{'Code': '123', 'Price': '1500'}, {'Code': '456', 'Price': '2000'}]
        >>> offer_ids = {'123', '456', '789'}
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> print(prices)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': 1500},
            {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '456', 'old_price': '0', 'price': 2000}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Converts a price string to a numeric string by removing non-numeric characters.

    Args:
        price (str): The price in string format, which may include currency symbols, commas, and decimal points.

    Returns:
        str: The numeric representation of the price, with all non-numeric characters removed.

    Example:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    Example of incorrect usage:
        >>> price_conversion("abc123")
        '123'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Splits the list into chunks of n elements.

    This function takes a list and divides it into sublists of fixed length.

    Arguments:
        lst (list): The list to be divided.
        n (int): The number of elements in each sublist.

    Returns:
        generator: A generator that yields sublists of length n.

    Example:
        >>> result = list(divide([1, 2, 3, 4, 5, 6], 2))
        >>> print(result)
        [[1, 2], [3, 4], [5, 6]]

    Note:
        If the length of the list is not divisible by n, the last sublist may contain fewer than n elements.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Uploads product prices to the Ozon platform.

    This function retrieves offer IDs from the Ozon API, creates a list of prices
    based on the provided watch remnants, and uploads the prices in batches of 1000.

    Args:
        watch_remnants (list): A list of dictionaries containing watch remnants
            with their corresponding codes and prices.
        client_id (str): The client ID for authenticating with the Ozon API.
        seller_token (str): The seller token for authenticating with the Ozon API.

    Returns:
        list: A list of price dictionaries that were successfully uploaded.

    Example:
        >>> watch_remnants = [{'Код': '123', 'Цена': '5\'990.00 руб.'}, ...]
        >>> client_id = 'your_client_id'
        >>> seller_token = 'your_seller_token'
        >>> prices = await upload_prices(watch_remnants, client_id, seller_token)
        >>> print(prices)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': '5990'}, ...]

    Raises:
        requests.exceptions.ReadTimeout: If the request to the Ozon API times out.
        requests.exceptions.ConnectionError: If there is a connection error while
            trying to reach the Ozon API.
        Exception: For any other exceptions that may occur during execution.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Uploads product stocks to the Ozon platform.

    This function retrieves offer IDs from the Ozon API, creates a list of stocks
    based on the provided watch remnants, and uploads the stocks in batches of 100.

    Args:
        watch_remnants (list): A list of dictionaries containing watch remnants
                                with their corresponding codes and stock quantities.
        client_id (str): The client ID for authenticating with the Ozon API.
        seller_token (str): The seller token for authenticating with the Ozon API.

    Returns:
        tuple: A tuple containing two lists:
            - list: A list of stock dictionaries that have a non-zero stock quantity.
            - list: A complete list of stock dictionaries that were processed.

    Example:
        >>> watch_remnants = [{'Код': '123', 'Остаток': 10}, ...]
        >>> client_id = 'your_client_id'
        >>> seller_token = 'your_seller_token'
        >>> not_empty, all_stocks = await upload_stocks(watch_remnants, client_id, seller_token)
        >>> print(not_empty)
        [{'offer_id': '123', 'stock': 10}, ...]

    Raises:
        requests.exceptions.ReadTimeout: If the request to the Ozon API times out.
        requests.exceptions.ConnectionError: If there is a connection error while
                                                trying to reach the Ozon API.
        Exception: For any other exceptions that may occur during execution.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
