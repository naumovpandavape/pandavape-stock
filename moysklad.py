import requests
import json
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import deque # Используем для построения иерархии

# --- Настройки ---
API_URL = "https://api.moysklad.ru/api/remap/1.2"
# !!! ВАЖНО: Используй свой реальный токен! Храни безопасно!
API_TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c" # ЗАМЕНИ НА СВОЙ ТОКЕН
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json"
}
OUTPUT_FILENAME = "stock_data.json"
ERROR_LOG_FILENAME = "error_log.txt"
REQUEST_DELAY = 0.2 # Можно немного уменьшить задержку
RETRY_COUNT = 3
BACKOFF_FACTOR = 1

# --- Фильтры ---
FILTER_ONLY_PRODUCTS = True
FILTER_ONLY_ACTIVE = True
# --- /Фильтры ---

# Настройка сессии
session = requests.Session()
retry_strategy = Retry(total=RETRY_COUNT, backoff_factor=BACKOFF_FACTOR, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "OPTIONS"])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.headers.update(HEADERS)

def log_error(message, response=None, level="ERROR"):
    """Логирование ошибок и предупреждений"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_prefix = f"[{timestamp}] {level}:"; error_msg = f"{log_prefix} {message}"
    if response is not None:
        response_text_preview = response.text[:500] if response.text else "No response body"
        error_msg += f"\n  URL: {response.url}\n  Status: {response.status_code}\n  Response: {response_text_preview}..."
    print(error_msg)
    try:
        with open(ERROR_LOG_FILENAME, "a", encoding="utf-8") as f: f.write(error_msg + "\n" + ("-"*20) + "\n")
    except IOError as e: print(f"[{timestamp}] CRITICAL: Не удалось записать в лог-файл {ERROR_LOG_FILENAME}: {e}")

def fetch_all_pages(endpoint, params=None, expand_params=None, filters=None):
    """Универсальная функция для получения всех данных с пагинацией"""
    url = f"{API_URL}/{endpoint}"; all_items = []
    current_params = params.copy() if params else {}
    if expand_params: current_params["expand"] = expand_params
    if filters: current_params["filter"] = ";".join(filters)
    req_count = 0
    print(f"\nНачало загрузки: {endpoint} (Фильтры: {filters if filters else 'Нет'}, Expand: {expand_params if expand_params else 'Нет'})")
    while url:
        req_count += 1; log_url = url.replace(API_TOKEN, "***TOKEN***")
        try:
            response = session.get(url, params=current_params)
            if not response.ok: print(f"    INFO: Response Status: {response.status_code} for {log_url.replace(API_URL,'')}")
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                for error in data["errors"]: log_error(f"API Error in {endpoint}: {error.get('error', 'Unknown')} (Param: {error.get('parameter', '')})", response)
            batch = data.get("rows", [])
            if not isinstance(batch, list): log_error(f"API Error: Ожидался список в 'rows', получен {type(batch)}", response); batch=[]
            all_items.extend(batch)
            if req_count == 1 or len(all_items) % 1000 < len(batch): print(f"    Загружено: {len(batch)} (Всего: {len(all_items)})")
            url = data.get("meta", {}).get("nextHref"); current_params = None
            if url: time.sleep(REQUEST_DELAY)
        except requests.exceptions.Timeout as e: log_error(f"Таймаут {endpoint}: {e}", getattr(e, 'response', None), level="WARN"); print("    Повтор..."); time.sleep(5)
        except requests.exceptions.RequestException as e:
            response = getattr(e, 'response', None)
            if response is not None and response.status_code == 401: log_error(f"401 Auth Error {endpoint}.", response); print("!!! ПРОВЕРЬТЕ ТОКЕН !!!"); return None
            elif response is not None and response.status_code == 403: log_error(f"403 Forbidden {endpoint}.", response, level="WARN"); print("!!! ПРОВЕРЬТЕ ПРАВА ТОКЕНА !!!") # Не прерываем
            else: log_error(f"Request Error {endpoint}: {e}", response, level="WARN"); print("    Проблема..."); time.sleep(5)
        except json.JSONDecodeError as e: log_error(f"JSON Decode Error {endpoint}: {e}", response); print("    Пропуск."); url = None
        except Exception as e: log_error(f"Unexpected Error {endpoint}: {type(e).__name__}: {e}", response); url = None
    print(f"Загрузка {endpoint} завершена. Всего: {len(all_items)}")
    return all_items

# *** НОВАЯ ФУНКЦИЯ: Загрузка и построение карты папок ***
def build_folder_hierarchy_map():
    """Загружает все папки товаров и строит карту иерархии."""
    print("\n--- Построение карты иерархии папок ---")
    all_folders_raw = fetch_all_pages(
        "entity/productfolder",
        params={"limit": 100}
        # expand=parent здесь не нужен, т.к. parent есть по умолчанию
    )
    if all_folders_raw is None: return None # Ошибка при загрузке

    folders_map = {} # { folder_id: {'name': '...', 'parent_id': '...'} }
    # Первый проход: собираем основную информацию
    for folder in all_folders_raw:
        try:
            meta = folder.get("meta")
            if not isinstance(meta, dict): continue
            href = meta.get("href")
            if not isinstance(href, str): continue
            folder_id = href.split('/')[-1]
            if not folder_id: continue

            name = folder.get("name", "Без имени")
            parent_id = None
            parent_data = folder.get("parent")
            if isinstance(parent_data, dict):
                parent_meta = parent_data.get("meta")
                if isinstance(parent_meta, dict):
                    parent_href = parent_meta.get("href")
                    if isinstance(parent_href, str):
                        parent_id = parent_href.split('/')[-1]

            folders_map[folder_id] = {'name': name, 'parent_id': parent_id}
        except Exception as e:
            log_error(f"Ошибка обработки папки при построении карты: {e}. Папка: {json.dumps(folder)}")
            continue

    print(f"Карта иерархии папок построена. Записей: {len(folders_map)}")
    return folders_map


def generate_stock_json():
    """Генерация итогового JSON файла"""
    print("\n=== Начало обработки данных ===")
    start_time = time.time()

    try:
        # 0. Строим карту иерархии папок ПЕРЕД всем остальным
        folder_map = build_folder_hierarchy_map()
        if folder_map is None:
            print("!!! КРИТИЧЕСКАЯ ОШИБКА: Не удалось построить карту папок. Прерывание.")
            return

        # 1. Получаем ассортимент - НЕ ТРЕБУЕТСЯ EXPAND для папок теперь
        assortment_filters = []
        if FILTER_ONLY_PRODUCTS: assortment_filters.append("type=product")
        if FILTER_ONLY_ACTIVE: assortment_filters.append("archived=false")

        assortment = fetch_all_pages(
            "entity/assortment",
            params={"limit": 100},
            expand_params=None, # Expand не нужен, только ID папки
            filters=assortment_filters if assortment_filters else None
        )
        if assortment is None: return
        if not assortment:
             print("\nВНИМАНИЕ: Ассортимент пуст."); with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f: json.dump([], f); return

        # 2. Получаем остатки
        stock_data = fetch_all_pages("report/stock/bystore", params={"limit": 100})
        if stock_data is None: return
        if not stock_data: print("\nВНИМАНИЕ: Данные по остаткам пусты.")

        # 3. Создаем словарь остатков (логика из твоего скрипта)
        print("\nСоздание словаря остатков...")
        stock_dict = {}; stock_processed_count = 0
        if stock_data:
            for stock in stock_data:
                try:
                    product_id_from_stock = stock.get("meta", {}).get("href", "").split("/")[-1].split("?")[0]
                    if product_id_from_stock:
                        stores_list = stock.get("stockByStore", [])
                        if isinstance(stores_list, list):
                             stock_dict.setdefault(product_id_from_stock, []).extend(stores_list); stock_processed_count += 1
                except Exception as e: log_error(f"Ошибка обработки записи остатка: {e}. Запись: {json.dumps(stock)}"); continue
            print(f"Словарь остатков создан. Обработано записей: {stock_processed_count}")
        else: print("Данные по остаткам отсутствуют, словарь остатков пуст.")

        # 4. Формируем итоговый результат
        print("\nФормирование итогового JSON...")
        result_list = []; processed_count = 0; products_skipped_no_article = 0; products_without_folder_info = 0

        for product in assortment:
            try:
                 processed_count += 1
                 product_id = product.get("id"); product_name = product.get("name", "Без названия"); product_code = product.get("code", "")
                 if not product_id: continue
                 article = product.get("article", "") or product_code
                 if not article: products_skipped_no_article += 1; continue

                 # --- Получение остатков (логика из твоего скрипта) ---
                 stores_output = []
                 for store_info in stock_dict.get(product_id, []):
                     if isinstance(store_info, dict):
                         qty = store_info.get("stock", 0.0)
                         try: qty = float(qty) if qty is not None else 0.0 # Возвращаем float
                         except (ValueError, TypeError): qty = 0.0
                         stores_output.append({"store": store_info.get("name", "?"), "quantity": qty})
                 final_stores = stores_output or [{"store": "Нет данных", "quantity": 0.0}] # Заглушка

                 # --- Получение категорий ИЗ КАРТЫ ИЕРАРХИИ ---
                 tilda_category = None
                 tilda_parent_category = None
                 folder_id = None
                 product_folder_meta = product.get("productFolder", {}).get("meta")

                 if isinstance(product_folder_meta, dict):
                      folder_href = product_folder_meta.get("href")
                      if isinstance(folder_href, str):
                           folder_id = folder_href.split('/')[-1]

                 if folder_id and folder_id in folder_map:
                      # Нашли папку товара в карте
                      folder_info = folder_map[folder_id]
                      tilda_category = folder_info.get('name')
                      parent_id = folder_info.get('parent_id')

                      if parent_id and parent_id in folder_map:
                           # Нашли родителя папки товара в карте
                           parent_info = folder_map[parent_id]
                           tilda_parent_category = parent_info.get('name')
                      # Если parent_id есть, но его нет в карте - это странно, можно залогировать
                      elif parent_id:
                           log_error(f"Родительская папка с ID '{parent_id}' для папки '{tilda_category}' (товар '{product_name}') не найдена в карте папок.", level="WARN")

                 elif folder_id:
                      # Папка у товара есть, но мы не нашли ее в нашей карте - странно
                      log_error(f"Папка с ID '{folder_id}' для товара '{product_name}' не найдена в карте папок.", level="WARN")
                      products_without_folder_info += 1
                 else:
                      # Товар вообще без папки
                      products_without_folder_info += 1


                 # --- Формирование объекта JSON (нужный порядок) ---
                 output_product = {
                      "id": product_id,
                      "name": str(product_name),
                      "code": str(product_code),
                      "article": str(article),
                      "tilda_category": str(tilda_category) if tilda_category is not None else None,
                      "tilda_parent_category": str(tilda_parent_category) if tilda_parent_category is not None else None,
                      "stores": final_stores
                 }
                 result_list.append(output_product)

            except Exception as e:
                 log_error(f"Крит. ошибка обработки товара ID {product.get('id', 'N/A')} '{product.get('name', 'N/A')}': {type(e).__name__}: {e}")
                 import traceback; traceback.print_exc(); continue

        print(f"\nФормирование JSON завершено. Товаров в списке: {len(result_list)}")
        if products_skipped_no_article > 0: print(f"  ПРЕДУПРЕЖДЕНИЕ: {products_skipped_no_article} товаров пропущено (нет article/code).")
        if products_without_folder_info > 0: print(f"  ПРЕДУПРЕЖДЕНИЕ: {products_without_folder_info} товаров не имеют папки или информация о папке не найдена.")

        # 5. Сохранение результата
        print(f"\nСохранение результата в файл: {OUTPUT_FILENAME}")
        try:
            with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f: json.dump(result_list, f, ensure_ascii=False, indent=2)
            print(f"Файл успешно сохранен.")
        except IOError as e: log_error(f"Не удалось сохранить файл {OUTPUT_FILENAME}: {e}")
        except Exception as e: log_error(f"Неожиданная ошибка при сохранении файла: {e}")

        print(f"\n=== Обработка полностью завершена ===")
        print(f"Итого товаров в файле: {len(result_list)}")
        end_time = time.time(); print(f"Общее время выполнения: {end_time - start_time:.2f} секунд")

    except Exception as e:
        log_error(f"КРИТИЧЕСКАЯ ОШИБКА выполнения скрипта: {type(e).__name__}: {e}"); import traceback; traceback.print_exc()
    finally:
        print("\nСкрипт завершил работу.")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__)); os.chdir(script_dir)
    print(f"Рабочая директория: {os.getcwd()}")
    generate_stock_json()
    # input("\nНажмите Enter для выхода...")
