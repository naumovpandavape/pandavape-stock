import requests
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Настройки
API_URL = "https://api.moysklad.ru/api/remap/1.2"
API_TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Encoding": "gzip"
}

# Настройка сессии
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

def get_all_assortment():
    url = f"{API_URL}/entity/assortment"
    params = {"limit": 100}
    all_products = []
    
    while url:
        try:
            response = session.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            data = response.json()
            all_products.extend(data.get("rows", []))
            url = data.get("meta", {}).get("nextHref")
            params = None
            time.sleep(0.5)
        except Exception as e:
            print(f"Ошибка при запросе товаров: {e}")
            time.sleep(5)
            continue
    return all_products

def get_all_stock():
    url = f"{API_URL}/report/stock/bystore"
    params = {"limit": 100}
    all_stock = []
    
    while url:
        try:
            response = session.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            data = response.json()
            all_stock.extend(data.get("rows", []))
            url = data.get("meta", {}).get("nextHref")
            params = None
            time.sleep(0.5)
        except Exception as e:
            print(f"Ошибка при запросе остатков: {e}")
            time.sleep(5)
            continue
    return all_stock

def generate_stock_json():
    print("Начало обработки данных...")
    start_time = time.time()
    
    # Параллельная загрузка данных
    import threading
    assortment = []
    stock_data = []
    
    def load_assortment():
        nonlocal assortment
        assortment = get_all_assortment()
    
    def load_stock():
        nonlocal stock_data
        stock_data = get_all_stock()
    
    t1 = threading.Thread(target=load_assortment)
    t2 = threading.Thread(target=load_stock)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Оптимизированная обработка
    stock_dict = {}
    for stock in stock_data:
        product_id = stock.get("meta", {}).get("href", "").split("/")[-1].split("?")[0]
        if product_id:
            stock_dict.setdefault(product_id, []).extend(stock.get("stockByStore", []))
    
    result = []
    for product in assortment:
        product_id = product.get("id")
        stores = []
        
        for store in stock_dict.get(product_id, []):
            stores.append({
                "store": store.get("name"),
                "quantity": store.get("stock", 0)
            })
        
        result.append({
            "id": product_id,
            "name": product.get("name"),
            "code": product.get("code"),
            "article": product.get("article"),
            "stores": stores or [{"store": "Нет данных", "quantity": 0}]
        })
    
    with open("stock_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    
    print(f"Успешно обработано: {len(result)} товаров за {time.time()-start_time:.2f} сек")

if __name__ == "__main__":
    generate_stock_json()
