import requests
import json
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Настройки (из моего скрипта) ---
API_URL = "https://api.moysklad.ru/api/remap/1.2"
# !!! ВАЖНО: Используй свой реальный токен! Храни безопасно!
API_TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c" # ЗАМЕНИ НА СВОЙ ТОКЕН
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json" # Добавлено для консистентности
}
OUTPUT_FILENAME = "stock_data.json"
ERROR_LOG_FILENAME = "error_log.txt"
REQUEST_DELAY = 0.3
RETRY_COUNT = 3
BACKOFF_FACTOR = 1

# --- Фильтры (из моего скрипта) ---
FILTER_ONLY_PRODUCTS = True
FILTER_ONLY_ACTIVE = True
# --- /Фильтры ---

# Настройка сессии (из моего скрипта)
session = requests.Session()
retry_strategy = Retry(
    total=RETRY_COUNT,
    backoff_factor=BACKOFF_FACTOR,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.headers.update(HEADERS) # Устанавливаем заголовки для сессии

def log_error(message, response=None, level="ERROR"):
    """Логирование ошибок и предупреждений (из моего скрипта)"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_prefix = f"[{timestamp}] {level}:"
    error_msg = f"{log_prefix} {message}"
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
    """Универсальная функция для получения всех данных с пагинацией (из моего скрипта)"""
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
        try:
            # Используем session из моего скрипта с retry и заголовками
            response = session.get(url, params=current_params) # НЕ используем headers=HEADERS здесь, т.к. они в сессии
            if not response.ok:
                 print(f"    INFO: Response Status: {response.status_code} for {log_url.replace(API_URL,'')}")

            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                for error in data["errors"]:
                    error_details = error.get('error', 'Unknown API error'); error_param = error.get('parameter', '')
                    log_error(f"API Error in {endpoint}: {error_details} (Parameter: {error_param})", response)

            batch = data.get("rows", [])
            if not isinstance(batch, list):
                 log_error(f"API Error: Ожидался список в 'rows', получен {type(batch)}", response); batch=[]

            all_items.extend(batch)
            if req_count == 1 or len(all_items) % 1000 < len(batch):
                 print(f"    Загружено: {len(batch)} (Всего: {len(all_items)})")

            url = data.get("meta", {}).get("nextHref")
            current_params = None # Параметры только в первом запросе

            if url:
                # Используем задержку из моего скрипта
                time.sleep(REQUEST_DELAY)

        except requests.exceptions.Timeout as e:
             log_error(f"Таймаут запроса {endpoint}: {e}", getattr(e, 'response', None), level="WARN")
             print("    Повторная попытка через 5 секунд..."); time.sleep(5)
        except requests.exceptions.RequestException as e:
            response = getattr(e, 'response', None)
            if response is not None and response.status_code == 401:
                 log_error(f"ОШИБКА АУТЕНТИФИКАЦИИ (401) для {endpoint}. Проверьте API_TOKEN.", response); return None
            elif response is not None and response.status_code == 403:
                 log_error(f"ОШИБКА ДОСТУПА (403) для {endpoint}. Проверьте права API токена.", response, level="WARN"); # Не прерываем
            else:
                 log_error(f"Ошибка запроса {endpoint}: {e}", response, level="WARN"); print("    Проблема с запросом..."); time.sleep(5)
        except json.JSONDecodeError as e:
             log_error(f"Ошибка декодирования JSON ответа {endpoint}: {e}", response); print("    Пропуск страницы."); url = None
        except Exception as e:
            log_error(f"Неожиданная ошибка при запросе {endpoint}: {type(e).__name__}: {e}", response); url = None

    print(f"Загрузка {endpoint} завершена. Всего записей: {len(all_items)}")
    return all_items

# Функция get_category_names (из моего скрипта)
def get_category_names(product_folder_data, product_name_for_debug):
    """Извлекает имя категории и имя родительской категории."""
    category_name = None; parent_category_name = None
    if isinstance(product_folder_data, dict):
        category_name = product_folder_data.get("name")
        parent_folder_data = product_folder_data.get("parent")
        if isinstance(parent_folder_data, dict):
            parent_category_name = parent_folder_data.get("name")
            # Логирование, если родитель есть, но без имени
            if not parent_category_name:
                 log_error(f"Имя родительской папки не найдено в объекте parent для товара '{product_name_for_debug}' в папке '{category_name}'.", level="WARN")
        # Логирование, если 'parent' есть, но не словарь (кроме None)
        elif parent_folder_data is not None:
             log_error(f"Поле 'parent' для папки '{category_name}' (товар '{product_name_for_debug}') не является словарем.", level="WARN")
    return category_name, parent_category_name


def generate_stock_json():
    """Генерация итогового JSON файла"""
    print("\n=== Начало обработки данных ===")
    start_time = time.time()

    try:
        # 1. Получаем ассортимент (логика из моего скрипта)
        assortment_filters = []
        if FILTER_ONLY_PRODUCTS: assortment_filters.append("type=product")
        if FILTER_ONLY_ACTIVE: assortment_filters.append("archived=false")
        expand_params_value = "productFolder,productFolder.parent"
        assortment = fetch_all_pages(
            "entity/assortment",
            params={"limit": 100}, expand_params=expand_params_value,
            filters=assortment_filters if assortment_filters else None
        )
        if assortment is None: return
        if not assortment:
             print("\nВНИМАНИЕ: Список ассортимента пуст.");
             with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f: json.dump([], f)
             return

        # 2. Получаем остатки (используем fetch_all_pages из моего скрипта)
        stock_data = fetch_all_pages(
             "report/stock/bystore",
             params={"limit": 100} # Параметры как в get_all_stock из твоего скрипта
        )
        if stock_data is None: return
        if not stock_data: print("\nВНИМАНИЕ: Данные по остаткам не получены или пусты.")


        # 3. Создаем словарь остатков (ЛОГИКА ИЗ ТВОЕГО СКРИПТА moysklad.py.txt)
        print("\nСоздание словаря остатков (логика из moysklad.py.txt)...")
        stock_dict = {}
        stock_processed_count = 0
        if stock_data:
            for stock in stock_data:
                try:
                    # Извлечение ID продукта из meta.href как в твоем скрипте
                    product_id_from_stock = stock.get("meta", {}).get("href", "").split("/")[-1].split("?")[0]
                    if product_id_from_stock:
                        # Получение списка складов
                        stores_list = stock.get("stockByStore", [])
                        if isinstance(stores_list, list):
                             # Добавляем данные по складам в словарь
                             # setdefault создает ключ, если его нет, и возвращает значение (список)
                             stock_dict.setdefault(product_id_from_stock, []).extend(stores_list)
                             stock_processed_count += 1 # Считаем успешно обработанные записи остатков
                        # else: log_error(...) # Можно добавить лог, если stockByStore не список

                except Exception as e:
                    # Логируем ошибку, но продолжаем обработку других записей остатков
                    log_error(f"Ошибка обработки записи остатка при создании словаря: {e}. Запись: {json.dumps(stock)}")
                    continue # Переходим к следующей записи в stock_data
            print(f"Словарь остатков создан. Обработано записей: {stock_processed_count}")
        else:
            print("Данные по остаткам отсутствуют, словарь остатков пуст.")


        # 4. Формируем итоговый результат (ЛОГИКА ИЗ МОЕГО СКРИПТА, но используем СТРУКТУРУ ОСТАТКОВ из твоего)
        print("\nФормирование итогового JSON...")
        result_list = []
        processed_count = 0
        products_skipped_no_article = 0
        parent_data_warnings = 0

        for product in assortment:
            try:
                 processed_count += 1
                 product_id = product.get("id")
                 product_name = product.get("name", "Без названия")
                 product_code = product.get("code", "")

                 if not product_id: continue

                 article = product.get("article", "") or product_code
                 if not article:
                     products_skipped_no_article += 1; continue

                 # --- Получение остатков (ЛОГИКА ИЗ ТВОЕГО СКРИПТА moysklad.py.txt) ---
                 stores_output = []
                 # Ищем остатки по ID товара в словаре, созданном твоей логикой
                 # stock_dict.get(product_id, []) вернет список складов или пустой список
                 for store_info in stock_dict.get(product_id, []):
                     # Проверяем, что store_info - это словарь (на всякий случай)
                     if isinstance(store_info, dict):
                         stores_output.append({
                             # Берем поля 'name' и 'stock' как в твоем скрипте
                             "store": store_info.get("name", "Неизвестный склад"),
                             "quantity": store_info.get("stock", 0.0) # Используем 0.0 как дефолт для float
                         })
                 # Применяем заглушку, если список складов пуст (как в твоем скрипте)
                 final_stores = stores_output or [{"store": "Нет данных", "quantity": 0.0}]
                 # --- Конец логики остатков из твоего скрипта ---


                 # --- Получение категорий (логика из моего скрипта) ---
                 tilda_category, tilda_parent_category = get_category_names(
                     product.get("productFolder"), product_name # Передаем имя для логов
                 )
                 # Считаем предупреждения, если они были при получении категорий
                 if tilda_category and not tilda_parent_category and isinstance(product.get("productFolder", {}).get("parent"), dict):
                      parent_data_warnings +=1 # Примерный подсчет для итога


                 # --- Формирование объекта JSON (порядок и поля как ты просил) ---
                 output_product = {
                      "id": product_id,
                      "name": str(product_name),
                      "code": str(product_code),
                      "article": str(article),
                      "tilda_category": str(tilda_category) if tilda_category is not None else None,
                      "tilda_parent_category": str(tilda_parent_category) if tilda_parent_category is not None else None,
                      "stores": final_stores # Используем остатки, сформированные логикой твоего скрипта
                 }
                 result_list.append(output_product)

            except Exception as e:
                 log_error(f"Критическая ошибка обработки товара ID {product.get('id', 'N/A')} '{product.get('name', 'N/A')}': {type(e).__name__}: {e}")
                 import traceback; traceback.print_exc(); continue # Логируем и пропускаем товар

        print(f"\nФормирование JSON завершено. Товаров в итоговом списке: {len(result_list)}")
        if products_skipped_no_article > 0: print(f"  ПРЕДУПРЕЖДЕНИЕ: {products_skipped_no_article} товаров пропущено (нет article/code).")
        if parent_data_warnings > 0: print(f"  ПРЕДУПРЕЖДЕНИЕ: Обнаружены проблемы с получением данных родительских папок ({parent_data_warnings}). Проверьте '{ERROR_LOG_FILENAME}'.")


        # 5. Сохранение результата (из моего скрипта)
        print(f"\nСохранение результата в файл: {OUTPUT_FILENAME}")
        try:
            with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
                json.dump(result_list, f, ensure_ascii=False, indent=2) # indent=2 для читаемости
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
