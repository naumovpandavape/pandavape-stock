import requests
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
from functools import lru_cache # For caching folder details

# Настройки
API_URL = "https://api.moysklad.ru/api/remap/1.2"
# WARNING: Avoid hardcoding tokens directly in scripts for production. Use environment variables or config files.
API_TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json" # Good practice to add
}

# Настройка сессии
session = requests.Session()
retry_strategy = Retry(
    total=5, # Increased retries slightly
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.headers.update(HEADERS) # Set headers for the whole session

# --- Helper Functions ---

def fetch_paginated_data(endpoint_url, params={"limit": 100}):
    """Fetches all data from a paginated MoySklad endpoint."""
    all_data = []
    url = endpoint_url
    current_params = params.copy() # Use a copy to avoid modifying the original

    print(f"Fetching data from {endpoint_url}...")
    page_count = 0
    while url:
        try:
            # print(f"Requesting: {url} with params: {current_params}") # Debugging line
            response = session.get(url, params=current_params)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            rows = data.get("rows", [])
            all_data.extend(rows)
            page_count += 1
            print(f"  Fetched page {page_count}, {len(rows)} items. Total items so far: {len(all_data)}")

            # Get the next URL, clear params if nextHref is present
            url = data.get("meta", {}).get("nextHref")
            current_params = None # Subsequent requests use the full nextHref URL

            time.sleep(0.2) # Be nice to the API, reduce slightly from 0.5
        except requests.exceptions.RequestException as e:
            print(f"  Error during request to {endpoint_url}: {e}")
            print(f"  Response status: {response.status_code if 'response' in locals() else 'N/A'}")
            print(f"  Response text: {response.text if 'response' in locals() else 'N/A'}")
            print("  Retrying after 5 seconds...")
            time.sleep(5)
            # Don't continue, the retry mechanism in the session should handle it
            # If retries fail, raise_for_status() will eventually raise the exception
        except json.JSONDecodeError as e:
            print(f"  Error decoding JSON from {endpoint_url}: {e}")
            print(f"  Response text: {response.text if 'response' in locals() else 'N/A'}")
            print("  Skipping this page attempt after delay...")
            time.sleep(5)
            # Decide if you want to break or try to continue (might lose data)
            # For now, let's retry getting the next page if possible
            if url: # If we already have a nextHref, try that
                current_params = None
                continue
            else: # If no nextHref, we might be stuck
                print("  Cannot proceed without a valid nextHref. Aborting fetch for this endpoint.")
                break # Stop fetching for this endpoint

    print(f"Finished fetching from {endpoint_url}. Total items: {len(all_data)}")
    return all_data

@lru_cache(maxsize=1024) # Cache results for folder URLs
def get_folder_details(folder_href):
    """Fetches folder name and pathName from its href, with caching."""
    if not folder_href:
        return None
    try:
        # print(f"  Fetching folder details: {folder_href}") # Debugging
        response = session.get(folder_href) # Headers are set on session
        response.raise_for_status()
        data = response.json()
        return {
            "name": data.get("name"),
            "pathName": data.get("pathName") # Full path like "Category / Subcategory"
        }
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching folder details for {folder_href}: {e}")
        # Return None or a default structure if needed
        return None
    except json.JSONDecodeError as e:
        print(f"  Error decoding JSON for folder {folder_href}: {e}")
        return None


def generate_stock_json():
    print("Начало обработки данных...")
    start_time = time.time()

    assortment_data = []
    stock_data = []
    threads = []

    print("Запуск параллельной загрузки данных (Товары и Остатки)...")
    def load_assortment():
        nonlocal assortment_data
        assortment_url = f"{API_URL}/entity/assortment"
        assortment_data = fetch_paginated_data(assortment_url)

    def load_stock():
        nonlocal stock_data
        stock_url = f"{API_URL}/report/stock/bystore"
        # Optional: Add filter for specific stores if needed
        # params = {"limit": 100, "filter": "store=STORE_HREF_1;store=STORE_HREF_2"}
        stock_data = fetch_paginated_data(stock_url)

    t1 = threading.Thread(target=load_assortment)
    t2 = threading.Thread(target=load_stock)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
    print("Загрузка данных завершена.")

    if not assortment_data:
        print("Ошибка: Не удалось загрузить данные о товарах (assortment). Выход.")
        return
    if not stock_data:
        print("Предупреждение: Не удалось загрузить данные об остатках (stock). Остатки не будут добавлены.")
        # Continue processing without stock if desired, or exit
        # return

    print("Обработка остатков...")
    stock_dict = {}
    for stock_item in stock_data:
        # The meta in stock report refers to the *assortment* item
        item_meta = stock_item.get("meta")
        if not item_meta: continue

        # Extract ID reliably from href (handles potential query parameters)
        href = item_meta.get("href", "")
        try:
            item_id = href.split("/")[-1].split("?")[0]
        except IndexError:
            print(f"  Warning: Could not parse ID from href: {href}")
            continue

        if item_id:
            # Ensure stockByStore is a list
            stores_stock_info = stock_item.get("stockByStore", [])
            if not isinstance(stores_stock_info, list):
                 print(f"  Warning: 'stockByStore' is not a list for item ID {item_id}. Skipping stock info.")
                 stores_stock_info = []

            # Aggregate stock info if product appears multiple times (shouldn't happen in 'bystore' normally)
            # More robust: Directly use the list provided
            stock_dict[item_id] = stores_stock_info # Overwrites if duplicate ID found, which is unusual for bystore report

    print("Обработка товаров и объединение данных...")
    result = []
    processed_count = 0
    total_assortment = len(assortment_data)
    for product in assortment_data:
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"  Обработано {processed_count}/{total_assortment} товаров...")

        product_id = product.get("id")
        if not product_id:
            print(f"  Warning: Skipping item without ID: {product.get('name', 'N/A')}")
            continue

        # --- Get Folder/Group Info ---
        folder_info = None
        group_name = None
        group_path = None
        product_folder_meta = product.get("productFolder", {}).get("meta")
        if product_folder_meta and product_folder_meta.get("href"):
            folder_href = product_folder_meta.get("href")
            # Use cached function to get details
            folder_info = get_folder_details(folder_href)
            if folder_info:
                group_name = folder_info.get("name")
                group_path = folder_info.get("pathName") # Full path

        # --- Get Stock Info ---
        product_stock_list = stock_dict.get(product_id, [])
        stores = []
        if product_stock_list:
             for store_stock in product_stock_list:
                 store_name = store_stock.get("name")
                 quantity = store_stock.get("stock", 0)
                 if store_name is not None: # Check if store name exists
                     stores.append({
                         "store": store_name,
                         "quantity": quantity
                     })
        else:
            # Keep structure consistent, even if no stock data found for this item
             stores.append({"store": "Нет данных об остатках", "quantity": 0})


        # --- Assemble Result Item ---
        result.append({
            "id": product_id,
            "name": product.get("name"),
            "code": product.get("code"),
            "article": product.get("article"),
            "group_name": group_name, # Immediate group name
            "group_path": group_path, # Full path like "Group / Subgroup"
            "stores": stores
        })

    print("Сохранение данных в JSON файл...")
    output_filename = "stock_data_with_groups.json"
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=4)
        print(f"Успешно сохранено: {len(result)} товаров в файл {output_filename}")
    except IOError as e:
        print(f"Ошибка записи в файл {output_filename}: {e}")


    end_time = time.time()
    print(f"Обработка завершена за {end_time - start_time:.2f} сек")
    # Print cache info for debugging/tuning
    print(f"Folder cache info: {get_folder_details.cache_info()}")

if __name__ == "__main__":
    generate_stock_json()
