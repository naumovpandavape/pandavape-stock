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

def log_error(message, response=None):
    """Логирование ошибок"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    error_msg = f"[{timestamp}] ERROR: {message}"
    if response:
        error_msg += f"\nURL: {response.url}\nStatus: {response.status_code}\nResponse: {response.text[:500]}"
    print(error_msg)
    with open("error_log.txt", "a", encoding="utf-8") as f:
        f.write(error_msg + "\n")

def get_all_assortment():
    """Получение всего ассортимента с группами товаров"""
    url = f"{API_URL}/entity/assortment"
    params = {
        "limit": 100,
        "expand": "productFolder,productFolder.parent"
    }
    all_products = []
    
    print("\nНачало загрузки ассортимента...")
    
    while url:
        try:
            response = session.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "errors" in data:
                for error in data["errors"]:
                    log_error(f"API Error: {error.get('error', 'Unknown error')}", response)
                break
                
            batch = data.get("rows", [])
            all_products.extend(batch)
            print(f"Загружено {len(batch)} товаров. Всего: {len(all_products)}")
            
            url = data.get("meta", {}).get("nextHref")
            params = None  # Для последующих запросов параметры уже в URL
            time.sleep(0.3)
            
        except requests.exceptions.RequestException as e:
            log_error(f"Ошибка запроса ассортимента: {str(e)}", getattr(e, 'response', None))
            time.sleep(5)
            continue
            
    return all_products

def get_all_stock():
    """Получение всех остатков"""
    url = f"{API_URL}/report/stock/bystore"
    params = {"limit": 100}
    all_stock = []
    
    print("\nНачало загрузки остатков...")
    
    while url:
        try:
            response = session.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "errors" in data:
                for error in data["errors"]:
                    log_error(f"API Error: {error.get('error', 'Unknown error')}", response)
                break
                
            batch = data.get("rows", [])
            all_stock.extend(batch)
            print(f"Загружено {len(batch)} остатков. Всего: {len(all_stock)}")
            
            url = data.get("meta", {}).get("nextHref")
            params = None
            time.sleep(0.3)
            
        except requests.exceptions.RequestException as e:
            log_error(f"Ошибка запроса остатков: {str(e)}", getattr(e, 'response', None))
            time.sleep(5)
            continue
            
    return all_stock

def get_group_hierarchy(product):
    """Получение иерархии групп для товара"""
    if "productFolder" not in product:
        return None
        
    try:
        group = product["productFolder"]
        hierarchy = [{
            "id": group.get("meta", {}).get("href", "").split("/")[-1],
            "name": group.get("name"),
            "code": group.get("code", "")
        }]
        
        # Добавляем родительские группы, если они есть
        parent = group.get("parent")
        while parent:
            hierarchy.append({
                "id": parent.get("meta", {}).get("href", "").split("/")[-1],
                "name": parent.get("name"),
                "code": parent.get("code", "")
            })
            parent = parent.get("parent")
            
        return hierarchy
        
    except Exception as e:
        log_error(f"Ошибка обработки группы товара: {str(e)}")
        return None

def generate_stock_json():
    """Генерация итогового JSON файла"""
    print("\n=== Начало обработки данных ===")
    start_time = time.time()
    
    try:
        # Последовательная загрузка для надежности
        assortment = get_all_assortment()
        stock_data = get_all_stock()
        
        # Создаем словарь остатков
        stock_dict = {}
        for stock in stock_data:
            try:
                product_id = stock.get("meta", {}).get("href", "").split("/")[-1]
                if product_id:
                    stock_dict.setdefault(product_id, []).extend(stock.get("stockByStore", []))
            except Exception as e:
                log_error(f"Ошибка обработки остатка: {str(e)}")
                continue
        
        # Формируем результат
        result = []
        for product in assortment:
            try:
                product_id = product.get("id")
                
                # Обработка остатков
                stores = []
                for store in stock_dict.get(product_id, []):
                    stores.append({
                        "store": store.get("name", "Неизвестный склад"),
                        "quantity": store.get("stock", 0)
                    })
                
                # Обработка групп
                groups = get_group_hierarchy(product)
                
                result.append({
                    "id": product_id,
                    "name": product.get("name", "Без названия"),
                    "code": product.get("code", ""),
                    "article": product.get("article", ""),
                    "stores": stores if stores else [{"store": "Нет данных", "quantity": 0}],
                    "groups": groups
                })
                
            except Exception as e:
                log_error(f"Ошибка обработки товара {product.get('id')}: {str(e)}")
                continue
        
        # Сохранение результата
        output_file = "stock_data.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=4)
        
        print(f"\n=== Обработка завершена ===")
        print(f"Обработано товаров: {len(result)}")
        print(f"Файл сохранен: {output_file}")
        print(f"Общее время: {time.time()-start_time:.2f} секунд")
        
    except Exception as e:
        log_error(f"Критическая ошибка: {str(e)}")
    finally:
        input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    generate_stock_json()
