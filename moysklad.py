import requests
import json
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# from collections import deque # deque больше не нужен

# --- Настройки ---
API_URL = "https://api.moysklad.ru/api/remap/1.2"
# !!! ВАЖНО: Используй свой реальный токен! Храни безопасно!
API_TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c" # ЗАМЕНИ НА СВОЙ ТОКЕН
HEADERS = {"Authorization": f"Bearer {API_TOKEN}", "Accept-Encoding": "gzip", "Content-Type": "application/json"}
OUTPUT_FILENAME = "stock_data.json"
ERROR_LOG_FILENAME = "error_log.txt"
REQUEST_DELAY = 0.2; RETRY_COUNT = 3; BACKOFF_FACTOR = 1
FILTER_ONLY_PRODUCTS = True; FILTER_ONLY_ACTIVE = True
# --- /Настройки ---

# Настройка сессии
session = requests.Session()
retry_strategy = Retry(total=RETRY_COUNT, backoff_factor=BACKOFF_FACTOR, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "OPTIONS"])
adapter = HTTPAdapter(max_retries=retry_strategy); session.mount("https://", adapter); session.headers.update(HEADERS)

def log_error(message, response=None, level="ERROR"):
    """Логирование ошибок и предупреждений"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S"); log_prefix = f"[{timestamp}] {level}:"; error_msg = f"{log_prefix} {message}"
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
    current_params = params.copy() if params else {}; req_count = 0
    if expand_params: current_params["expand"] = expand_params
    if filters: current_params["filter"] = ";".join(filters)
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
            elif response is not None and response.status_code == 403: log_error(f"403 Forbidden {endpoint}.", response, level="WARN"); print("!!! ПРОВЕРЬТЕ ПРАВА ТОКЕНА !!!")
            else: log_error(f"Request Error {endpoint}: {e}", response, level="WARN"); print("    Проблема..."); time.sleep(5)
        except json.JSONDecodeError as e: log_error(f"JSON Decode Error {endpoint}: {e}", response); print("    Пропуск."); url = None
        except Exception as e: log_error(f"Unexpected Error {endpoint}: {type(e).__name__}: {e}", response); url = None
    print(f"Загрузка {endpoint} завершена. Всего: {len(all_items)}")
    return all_items

# *** ФУНКЦИЯ С УСИЛЕННОЙ ОТЛАДКОЙ ***
def build_folder_hierarchy_map():
    """Загружает все папки товаров и строит карту иерархии."""
    print("\n--- Построение карты иерархии папок ---")
    all_folders_raw = fetch_all_pages("entity/productfolder", params={"limit": 100})
    if all_folders_raw is None: return None

    folders_map = {}
    processed_folders_count = 0
    print("--- Начало детальной отладки обработки папок ---")
    for folder in all_folders_raw:
        processed_folders_count += 1
        folder_id = None; name = None; parent_id = None; parent_data_debug = None

        try:
            # Извлекаем ID папки
            meta = folder.get("meta")
            if isinstance(meta, dict):
                href = meta.get("href")
                if isinstance(href, str): folder_id = href.split('/')[-1]

            if not folder_id:
                print(f"DEBUG Folder {processed_folders_count}: Пропуск папки - не найден ID. Данные: {json.dumps(folder)}")
                continue

            # Извлекаем имя
            name = folder.get("name", "Без имени")

            # Извлекаем данные родителя
            parent_data = folder.get("parent")
            parent_data_debug = parent_data # Сохраняем как есть для отладки

            if isinstance(parent_data, dict):
                # Если родитель есть и это словарь, пытаемся извлечь его ID
                parent_meta = parent_data.get("meta")
                if isinstance(parent_meta, dict):
                    parent_href = parent_meta.get("href")
                    if isinstance(parent_href, str): parent_id = parent_href.split('/')[-1]

            # Выводим отладочную информацию для КАЖДОЙ папки
            print(f"DEBUG Folder {processed_folders_count}: ID='{folder_id}', Name='{name}', Extracted ParentID='{parent_id}', Raw Parent Data={json.dumps(parent_data_debug)}")

            # Добавляем в карту
            folders_map[folder_id] = {'name': name, 'parent_id': parent_id}

        except Exception as e:
            log_error(f"Ошибка обработки папки при построении карты: {e}. Папка: {json.dumps(folder)}")
            continue

    print("--- Конец детальной отладки обработки папок ---")
    print(f"Карта иерархии папок построена. Обработано папок: {processed_folders_count}. Записей в карте: {len(folders_map)}")
    # Дополнительная проверка: выведем несколько записей из карты
    print("\n--- Пример записей из созданной карты папок (первые 5): ---")
    count = 0
    for f_id, f_data in folders_map.items():
        print(f"  ID: {f_id} => Name: {f_data.get('name')}, ParentID: {f_data.get('parent_id')}")
        count += 1
        if count >= 5: break
    print("--- ---")

    return folders_map


def generate_stock_json():
    """Генерация итогового JSON файла"""
    print("\n=== Начало обработки данных ===")
    start_time = time.time()
    try:
        folder_map = build_folder_hierarchy_map()
        if folder_map is None: print("!!! КРИТИЧЕСКАЯ ОШИБКА: Не удалось построить карту папок."); return

        assortment_filters = []; expand_params_value = None
        if FILTER_ONLY_PRODUCTS: assortment_filters.append("type=product")
        if FILTER_ONLY_ACTIVE: assortment_filters.append("archived=false")
        assortment = fetch_all_pages("entity/assortment", params={"limit": 100}, expand_params=expand_params_value, filters=assortment_filters if assortment_filters else None)
        if assortment is None: print("!!! КРИТИЧЕСКАЯ ОШИБКА: Не удалось получить ассортимент."); return
        elif not assortment:
            print("\nВНИМАНИЕ: Список ассортимента пуст.");
            try:
                with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f: json.dump([], f)
                print(f"Создан пустой файл: {OUTPUT_FILENAME}")
            except IOError as e: log_error(f"Не удалось создать пустой файл {OUTPUT_FILENAME}: {e}")
            return

        stock_data = fetch_all_pages("report/stock/bystore", params={"limit": 100})
        if stock_data is None: print("!!! КРИТИЧЕСКАЯ ОШИБКА: Не удалось получить остатки."); return
        if not stock_data: print("\nВНИМАНИЕ: Данные по остаткам пусты.")

        print("\nСоздание словаря остатков..."); stock_dict = {}; stock_processed_count = 0
        if stock_data:
            for stock in stock_data:
                try:
                    product_id_from_stock = stock.get("meta", {}).get("href", "").split("/")[-1].split("?")[0]
                    if product_id_from_stock:
                        stores_list = stock.get("stockByStore", [])
                        if isinstance(stores_list, list): stock_dict.setdefault(product_id_from_stock, []).extend(stores_list); stock_processed_count += 1
                except Exception as e: log_error(f"Ошибка обработки записи остатка: {e}. Запись: {json.dumps(stock)}"); continue
            print(f"Словарь остатков создан. Обработано записей: {stock_processed_count}")
        else: print("Данные по остаткам отсутствуют.")

        print("\nФормирование итогового JSON...")
        result_list = []; processed_count = 0; products_skipped_no_article = 0; products_without_folder_info = 0
        for product in assortment:
            try:
                 processed_count += 1
                 product_id = product.get("id"); product_name = product.get("name", "Без названия"); product_code = product.get("code", "")
                 if not product_id: continue
                 article = product.get("article", "") or product_code
                 if not article: products_skipped_no_article += 1; continue

                 stores_output = []
                 for store_info in stock_dict.get(product_id, []):
                     if isinstance(store_info, dict):
                         qty = store_info.get("stock", 0.0); try: qty = float(qty) if qty is not None else 0.0
                         except (ValueError, TypeError): qty = 0.0
                         stores_output.append({"store": store_info.get("name", "?"), "quantity": qty})
                 final_stores = stores_output or [{"store": "Нет данных", "quantity": 0.0}]

                 tilda_category = None; tilda_parent_category = None; folder_id = None
                 product_folder_meta = product.get("productFolder", {}).get("meta")
                 if isinstance(product_folder_meta, dict):
                      folder_href = product_folder_meta.get("href")
                      if isinstance(folder_href, str): folder_id = folder_href.split('/')[-1]
                 if folder_id and folder_id in folder_map:
                      folder_info = folder_map[folder_id]; tilda_category = folder_info.get('name'); parent_id = folder_info.get('parent_id')
                      if parent_id and parent_id in folder_map: parent_info = folder_map[parent_id]; tilda_parent_category = parent_info.get('name')
                      # elif parent_id: log_error(f"Родитель ID '{parent_id}' папки '{tilda_category}' не найден в карте.", level="WARN") # Раскомментировать для отладки, если родитель все еще null
                 elif folder_id: log_error(f"Папка ID '{folder_id}' товара '{product_name}' не найдена в карте.", level="WARN"); products_without_folder_info += 1
                 else: products_without_folder_info += 1

                 output_product = {
                      "id": product_id, "name": str(product_name), "code": str(product_code), "article": str(article),
                      "tilda_category": str(tilda_category) if tilda_category is not None else None,
                      "tilda_parent_category": str(tilda_parent_category) if tilda_parent_category is not None else None,
                      "stores": final_stores
                 }
                 result_list.append(output_product)
            except Exception as e: log_error(f"Крит. ошибка товара ID {product.get('id', 'N/A')} '{product.get('name', 'N/A')}': {type(e).__name__}: {e}"); import traceback; traceback.print_exc(); continue

        print(f"\nФормирование JSON завершено. Товаров: {len(result_list)}")
        if products_skipped_no_article > 0: print(f"  ПРЕДУПРЕЖДЕНИЕ: {products_skipped_no_article} пропущено (нет article/code).")
        if products_without_folder_info > 0: print(f"  ПРЕДУПРЕЖДЕНИЕ: {products_without_folder_info} без папки или инфо о папке не найдено.")

        print(f"\nСохранение файла: {OUTPUT_FILENAME}")
        try:
            with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f: json.dump(result_list, f, ensure_ascii=False, indent=2)
            print(f"Файл успешно сохранен.")
        except IOError as e: log_error(f"Не удалось сохранить файл {OUTPUT_FILENAME}: {e}")
        except Exception as e: log_error(f"Неожиданная ошибка при сохранении файла: {e}")

        print(f"\n=== Обработка полностью завершена ==="); print(f"Итого товаров в файле: {len(result_list)}")
        end_time = time.time(); print(f"Общее время: {end_time - start_time:.2f} секунд")
    except Exception as e: log_error(f"КРИТИЧЕСКАЯ ОШИБКА: {type(e).__name__}: {e}"); import traceback; traceback.print_exc()
    finally: print("\nСкрипт завершил работу.")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__)); os.chdir(script_dir)
    print(f"Рабочая директория: {os.getcwd()}")
    generate_stock_json()
    # input("\nНажмите Enter...")
