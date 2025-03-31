import requests
import json
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
REQUEST_DELAY = 0.3
RETRY_COUNT = 3
BACKOFF_FACTOR = 1

# --- Фильтры (можно изменить) ---
FILTER_ONLY_PRODUCTS = True
FILTER_ONLY_ACTIVE = True
# --- /Фильтры ---

# Настройка сессии (код без изменений)
session = requests.Session()
retry_strategy = Retry(
    total=RETRY_COUNT,
    backoff_factor=BACKOFF_FACTOR,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.headers.update(HEADERS)

def log_error(message, response=None):
    """Логирование ошибок (код без изменений)"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    error_msg = f"[{timestamp}] ERROR: {message}"
    if response is not None:
        response_text_preview = response.text[:500] if response.text else "No response body"
        error_msg += f"\n  URL: {response.url}\n  Status: {response.status_code}\n  Response: {response_text_preview}..."
    print(error_msg)
    try:
        with open(ERROR_LOG_FILENAME, "a", encoding="utf-8") as f:
            f.write(error_msg + "\n" + ("-"*20) + "\n")
    except IOError as e:
        print(f"[{timestamp}] CRITICAL: Не удалось записать в лог-файл {ERROR_LOG_FILENAME}: {e}")

def fetch_all_pages(endpoint, params=None, expand_params=None, filters=None):
    """Универсальная функция для получения всех данных с пагинацией (код без изменений)"""
    url = f"{API_URL}/{endpoint}"
    current_params = params.copy() if params else {}
    if expand_params:
         current_params["expand"] = expand_params
    if filters:
        filter_str = ";".join(filters)
        current_params["filter"] = filter_str

    all_items = []
    req_count = 0
    print(f"\nНачало загрузки: {endpoint} (Фильтры: {filters if filters else 'Нет'}, Expand: {expand_params if expand_params else 'Нет'})")

    while url:
        req_count += 1
        log_url = url.replace(API_TOKEN, "***TOKEN***")
        # print(f"  Запрос {req_count}: {log_url.replace(API_URL,'')}...") # Сделаем вывод менее подробным
        try:
            response = session.get(url, params=current_params)
            if not response.ok:
                 print(f"    WARNING: Response Status: {response.status_code} for {log_url.replace(API_URL,'')}")

            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                for error in data["errors"]:
                    error_details = error.get('error', 'Unknown API error')
                    error_param = error.get('parameter', '')
                    log_error(f"API Error in {endpoint}: {error_details} (Parameter: {error_param})", response)

            batch = data.get("rows", [])
            if not isinstance(batch, list):
                 log_error(f"API Error: Ожидался список в 'rows', получен {type(batch)}", response)
                 batch=[]

            all_items.extend(batch)
            # Уменьшим частоту вывода прогресса
            if req_count == 1 or len(all_items) % 1000 < len(batch): # Выводим на первой странице и примерно каждую 1000-ю запись
                 print(f"    Загружено: {len(batch)} (Всего: {len(all_items)})")

            url = data.get("meta", {}).get("nextHref")
            current_params = None

            if url:
                time.sleep(REQUEST_DELAY)

        except requests.exceptions.Timeout as e:
             log_error(f"Таймаут запроса {endpoint}: {e}", getattr(e, 'response', None))
             print("    Повторная попытка через 5 секунд...")
             time.sleep(5)
        except requests.exceptions.RequestException as e:
            if getattr(e, 'response', None) is not None and e.response.status_code == 401:
                 log_error(f"ОШИБКА АУТЕНТИФИКАЦИИ (401) для {endpoint}. Проверьте правильность API_TOKEN.", e.response)
                 print("!!! ПРОВЕРЬТЕ API ТОКЕН !!!")
                 return None
            elif getattr(e, 'response', None) is not None and e.response.status_code == 403:
                 log_error(f"ОШИБКА ДОСТУПА (403) для {endpoint}. Проверьте права API токена.", e.response)
                 print("!!! ПРОВЕРЬТЕ ПРАВА API ТОКЕНА !!!")
                 # return None # Реши, прерывать ли выполнение
            else:
                 log_error(f"Ошибка запроса {endpoint}: {e}", getattr(e, 'response', None))
                 print("    Проблема с запросом. Ожидание 5 секунд перед возможным повтором...")
                 time.sleep(5)

        except json.JSONDecodeError as e:
             log_error(f"Ошибка декодирования JSON ответа {endpoint}: {e}", response)
             print("    Не удалось разобрать ответ сервера. Пропуск страницы.")
             url = None
        except Exception as e:
            log_error(f"Неожиданная ошибка при запросе {endpoint}: {type(e).__name__}: {e}", response)
            url = None

    print(f"Загрузка {endpoint} завершена. Всего записей: {len(all_items)}")
    return all_items

def get_category_names(product_folder_data):
    """Извлекает имя категории и имя родительской категории."""
    category_name = None
    parent_category_name = None

    if isinstance(product_folder_data, dict):
        category_name = product_folder_data.get("name")
        parent_folder_data = product_folder_data.get("parent")
        if isinstance(parent_folder_data, dict):
            parent_category_name = parent_folder_data.get("name")
            # --- ОТЛАДКА ---
            # Раскомментируй, если родитель все еще null, чтобы увидеть всю структуру родителя
        print(f"    DEBUG Parent Data for {category_name}: {json.dumps(parent_folder_data, ensure_ascii=False, indent=2)}")
            # --- КОНЕЦ ОТЛАДКИ ---

    return category_name, parent_category_name


def generate_stock_json():
    """Генерация итогового JSON файла"""
    print("\n=== Начало обработки данных ===")
    start_time = time.time()

    try:
        # 1. Получаем ассортимент с запросом ДВУХ уровней родительских папок
        assortment_filters = []
        if FILTER_ONLY_PRODUCTS:
            assortment_filters.append("type=product")
        if FILTER_ONLY_ACTIVE:
            assortment_filters.append("archived=false")

        # *** ИЗМЕНЕНИЕ ЗДЕСЬ: Пробуем запросить больше уровней вложенности ***
        expand_params_value = "productFolder,productFolder.parent,productFolder.parent.parent"

        assortment = fetch_all_pages(
            "entity/assortment",
            params={"limit": 100},
            expand_params=expand_params_value, # Используем новый expand
            filters=assortment_filters if assortment_filters else None
        )

        if assortment is None:
             print("\n!!! КРИТИЧЕСКАЯ ОШИБКА: Не удалось получить данные ассортимента. Прерывание.")
             return
        elif not assortment:
             print("\nВНИМАНИЕ: Список ассортимента пуст. Файл JSON будет пустым.")
             with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f: json.dump([], f)
             return

        # 2. Получаем остатки (код без изменений)
        stock_data = fetch_all_pages(
             "report/stock/bystore",
             params={"limit": 100}
        )

        if stock_data is None:
             print("\n!!! КРИТИЧЕСКАЯ ОШИБКА: Не удалось получить данные об остатках. Прерывание.")
             return
        elif not stock_data:
             print("\nВНИМАНИЕ: Данные по остаткам не получены или пусты. Поле 'stores' будет пустым.")


        # 3. Создаем словарь остатков
        print("\nСоздание словаря остатков...")
        stock_dict = {}
        stock_processed_count = 0
        if stock_data:
            for stock_item in stock_data:
                try:
                    meta = stock_item.get("meta")
                    if not isinstance(meta, dict): continue
                    href = meta.get("href")
                    if not isinstance(href, str): continue
                    product_id_from_stock = href.split('/')[-1]
                    if not product_id_from_stock: continue

                    stores_list = stock_item.get("stockByStore", [])
                    if not isinstance(stores_list, list): continue

                    current_product_stores = []
                    for store_info in stores_list:
                         if not isinstance(store_info, dict): continue
                         store_name = store_info.get("name", "Неизвестный склад")
                         quantity_val = store_info.get("stock") # Получаем значение

                         # *** ИЗМЕНЕНИЕ ЗДЕСЬ: Возвращаем float для quantity ***
                         try:
                             # Преобразуем в float, если значение есть, иначе 0.0
                             quantity = float(quantity_val) if quantity_val is not None else 0.0
                         except (ValueError, TypeError):
                             quantity = 0.0 # По умолчанию 0.0 при ошибке

                         current_product_stores.append({
                             "store": str(store_name),
                             "quantity": quantity # Сохраняем как float
                         })

                    stock_dict.setdefault(product_id_from_stock, []).extend(current_product_stores)
                    stock_processed_count += 1
                except Exception as e:
                    log_error(f"Ошибка обработки записи остатка: {e}. Запись: {json.dumps(stock_item)}")
                    continue
            print(f"Словарь остатков создан. Обработано записей остатков: {stock_processed_count}")
        else:
            print("Данные по остаткам отсутствуют, словарь остатков пуст.")


        # 4. Формируем итоговый результат
        print("\nФормирование итогового JSON...")
        result_list = []
        processed_count = 0
        products_skipped_no_article = 0

        for product in assortment:
            try:
                 processed_count += 1
                 product_id = product.get("id")
                 product_name = product.get("name", "Без названия")
                 product_code = product.get("code", "")

                 if not product_id: continue

                 article = product.get("article", "") or product_code
                 if not article:
                     products_skipped_no_article += 1
                     continue

                 product_stores = stock_dict.get(product_id, [])

                 # *** ИЗМЕНЕНИЕ ЗДЕСЬ: Используем новую функцию для получения имен категорий ***
                 tilda_category, tilda_parent_category = get_category_names(product.get("productFolder"))

                 # Создаем объект в нужном порядке
                 output_product = {
                      "id": product_id,
                      "name": str(product_name),
                      "code": str(product_code),
                      "article": str(article),
                      "tilda_category": str(tilda_category) if tilda_category is not None else None,
                      "tilda_parent_category": str(tilda_parent_category) if tilda_parent_category is not None else None,
                      "stores": product_stores
                 }
                 result_list.append(output_product)

            except Exception as e:
                 log_error(f"Критическая ошибка обработки товара ID {product.get('id', 'N/A')} '{product.get('name', 'N/A')}': {type(e).__name__}: {e}")
                 import traceback
                 traceback.print_exc()
                 continue

        print(f"\nФормирование JSON завершено. Товаров в итоговом списке: {len(result_list)}")
        if products_skipped_no_article > 0:
             print(f"  ПРЕДУПРЕЖДЕНИЕ: {products_skipped_no_article} товаров были пропущены из-за отсутствия артикула/кода.")


        # 5. Сохранение результата (код без изменений)
        print(f"\nСохранение результата в файл: {OUTPUT_FILENAME}")
        try:
            with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
                json.dump(result_list, f, ensure_ascii=False, indent=2)
            print(f"Файл успешно сохранен.")
        except IOError as e:
             log_error(f"Не удалось сохранить файл {OUTPUT_FILENAME}: {e}")
        except Exception as e:
             log_error(f"Неожиданная ошибка при сохранении файла: {e}")


        print(f"\n=== Обработка полностью завершена ===")
        print(f"Итого товаров в файле: {len(result_list)}")
        end_time = time.time()
        print(f"Общее время выполнения: {end_time - start_time:.2f} секунд")

    except Exception as e:
        log_error(f"КРИТИЧЕСКАЯ ОШИБКА выполнения скрипта: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nСкрипт завершил работу.")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    print(f"Рабочая директория: {os.getcwd()}")

    generate_stock_json()
    # input("\nНажмите Enter для выхода...")
