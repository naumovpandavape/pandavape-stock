import requests
import json
import time
import os # Добавим для работы с путями к файлам
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Настройки ---
API_URL = "https://api.moysklad.ru/api/remap/1.2"
# !!! ВАЖНО: Храните токен безопасно. Не загружайте его в публичные репозитории!
# Лучше использовать переменные окружения или другие методы.
API_TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c" # ВАШ ТОКЕН
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json" # Добавим на всякий случай
}
OUTPUT_FILENAME = "stock_data.json" # Имя выходного файла
ERROR_LOG_FILENAME = "error_log.txt" # Имя файла лога ошибок
REQUEST_DELAY = 0.3 # Задержка между запросами API (в секундах)
RETRY_COUNT = 3 # Количество попыток повторного запроса
BACKOFF_FACTOR = 1 # Коэффициент экспоненциальной задержки
# --- /Настройки ---

# Настройка сессии с повторными попытками
session = requests.Session()
retry_strategy = Retry(
    total=RETRY_COUNT,
    backoff_factor=BACKOFF_FACTOR,
    status_forcelist=[429, 500, 502, 503, 504], # Статусы для повтора
    allowed_methods=["HEAD", "GET", "OPTIONS"] # Методы для повтора
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.headers.update(HEADERS) # Устанавливаем заголовки для всей сессии

def log_error(message, response=None):
    """Логирование ошибок"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    error_msg = f"[{timestamp}] ERROR: {message}"
    if response is not None:
        # Ограничиваем длину ответа для лога
        response_text_preview = response.text[:500] if response.text else "No response body"
        error_msg += f"\n  URL: {response.url}\n  Status: {response.status_code}\n  Response: {response_text_preview}..."
    print(error_msg) # Выводим в консоль
    try:
        with open(ERROR_LOG_FILENAME, "a", encoding="utf-8") as f:
            f.write(error_msg + "\n" + ("-"*20) + "\n")
    except IOError as e:
        print(f"[{timestamp}] CRITICAL: Не удалось записать в лог-файл {ERROR_LOG_FILENAME}: {e}")

def fetch_all_pages(endpoint, params=None, expand_params=None):
    """Универсальная функция для получения всех данных с пагинацией."""
    url = f"{API_URL}/{endpoint}"
    current_params = params.copy() if params else {}
    if expand_params:
         current_params["expand"] = expand_params # Добавляем expand в параметры

    all_items = []
    req_count = 0
    print(f"\nНачало загрузки: {endpoint}")

    while url:
        req_count += 1
        print(f"  Запрос {req_count}: {url.replace(API_URL,'')}...")
        try:
            response = session.get(url, params=current_params) # Параметры передаем только в первом запросе
            response.raise_for_status() # Проверка на HTTP ошибки (4xx, 5xx)
            data = response.json()

            # Проверка на ошибки API МойСклад в ответе
            if "errors" in data:
                for error in data["errors"]:
                    error_details = error.get('error', 'Unknown API error')
                    error_param = error.get('parameter', '')
                    log_error(f"API Error in {endpoint}: {error_details} (Parameter: {error_param})", response)
                # Не прерываем цикл, но логируем ошибку
                # Можно добавить break, если ошибки критичны

            batch = data.get("rows", [])
            if not isinstance(batch, list):
                 log_error(f"API Error: Ожидался список в 'rows', получен {type(batch)}", response)
                 batch=[] # Предотвращаем ошибку дальше

            all_items.extend(batch)
            print(f"    Загружено: {len(batch)} (Всего: {len(all_items)})")

            # Получаем URL следующей страницы
            url = data.get("meta", {}).get("nextHref")
            current_params = None  # Для последующих запросов параметры уже включены в URL

            if url:
                time.sleep(REQUEST_DELAY) # Задержка перед следующим запросом

        except requests.exceptions.Timeout as e:
             log_error(f"Таймаут запроса {endpoint}: {e}", getattr(e, 'response', None))
             print("    Повторная попытка через 5 секунд...")
             time.sleep(5)
             # Сессия сама повторит запрос благодаря Retry стратегии
        except requests.exceptions.RequestException as e:
            log_error(f"Ошибка запроса {endpoint}: {e}", getattr(e, 'response', None))
            print("    Проблема с запросом. Ожидание 5 секунд перед возможным повтором...")
            time.sleep(5) # Даем время перед автоматическим повтором сессии
            # Если Retry исчерпан, цикл прервется на raise_for_status в след. итерации

        except json.JSONDecodeError as e:
             log_error(f"Ошибка декодирования JSON ответа {endpoint}: {e}", response)
             print("    Не удалось разобрать ответ сервера. Пропуск страницы.")
             url = None # Прерываем пагинацию, так как ответ невалиден
        except Exception as e: # Ловим другие неожиданные ошибки
            log_error(f"Неожиданная ошибка при запросе {endpoint}: {e}", response)
            url = None # Прерываем на всякий случай

    print(f"Загрузка {endpoint} завершена. Всего записей: {len(all_items)}")
    return all_items

# Функция get_group_hierarchy больше не нужна для целевого JSON,
# так как expand=productFolder,productFolder.parent дает нам нужные данные сразу.
# Оставим ее закомментированной на случай, если понадобится сложная иерархия.
# def get_group_hierarchy(product_folder_data):
#     """(НЕ ИСПОЛЬЗУЕТСЯ В ТЕКУЩЕЙ ВЕРСИИ) Получение иерархии групп для товара"""
#     # ... (старый код функции) ...

def generate_stock_json():
    """Генерация итогового JSON файла"""
    print("\n=== Начало обработки данных ===")
    start_time = time.time()

    try:
        # 1. Получаем весь ассортимент с нужными развернутыми полями
        # Важно: productFolder - папка товара, productFolder.parent - родитель папки товара
        assortment = fetch_all_pages(
            "entity/assortment",
            params={"limit": 100}, # Стандартный лимит
             # Фильтр можно добавить сюда, если нужно (напр., только активные)
             # "filter": "archived=false"
            expand_params="productFolder,productFolder.parent"
        )

        # 2. Получаем все остатки по складам
        stock_data = fetch_all_pages(
             "report/stock/bystore",
             params={"limit": 100}
             # Можно добавить фильтр по ID складов, если нужны не все
             # "filter": "store.id=STORE_ID_1;store.id=STORE_ID_2"
        )

        if not assortment:
            print("Не удалось загрузить ассортимент. Проверьте токен и доступ к API.")
            return
        if not stock_data:
             print("Не удалось загрузить остатки. Проверьте токен и доступ к API.")
             # Можно продолжить без остатков, но JSON будет неполным
             # return

        # 3. Создаем словарь остатков для быстрого доступа
        # Ключ - ID товара (из assortment), Значение - список складов с остатками
        print("\nСоздание словаря остатков...")
        stock_dict = {}
        stock_processed_count = 0
        for stock_item in stock_data:
            try:
                # Получаем ID товара из ссылки meta.href
                meta = stock_item.get("meta")
                if not meta or not isinstance(meta, dict): continue # Пропускаем, если нет meta
                href = meta.get("href")
                if not href or not isinstance(href, str): continue # Пропускаем, если нет href
                # Извлекаем UUID из URL (последняя часть)
                product_id = href.split('/')[-1]
                if not product_id: continue # Пропускаем, если ID не извлечен

                stores_list = stock_item.get("stockByStore", [])
                if not isinstance(stores_list, list): continue # Пропускаем, если формат некорректен

                current_product_stores = []
                for store_info in stores_list:
                     if not isinstance(store_info, dict): continue # Пропускаем невалидный формат склада
                     store_name = store_info.get("name", "Неизвестный склад")
                     quantity = store_info.get("stock", 0)
                     # Проверяем, что quantity - число, иначе ставим 0
                     if not isinstance(quantity, (int, float)):
                         quantity = 0
                     current_product_stores.append({
                         "store": str(store_name), # Приводим к строке на всякий случай
                         "quantity": int(quantity) # Приводим к int
                     })

                # Добавляем или обновляем данные по товару
                if product_id in stock_dict:
                     # Такое маловероятно для stock/bystore, но на всякий случай
                     stock_dict[product_id].extend(current_product_stores)
                else:
                     stock_dict[product_id] = current_product_stores
                stock_processed_count += 1

            except Exception as e:
                # Логируем ошибку обработки конкретной записи остатка
                log_error(f"Ошибка обработки записи остатка: {e}. Запись: {json.dumps(stock_item)}")
                continue # Продолжаем со следующей записью

        print(f"Словарь остатков создан. Обработано записей остатков: {stock_processed_count}")


        # 4. Формируем итоговый результат
        print("\nФормирование итогового JSON...")
        result_list = []
        processed_count = 0
        products_without_article = 0

        for product in assortment:
            try:
                 processed_count += 1
                 product_id = product.get("id")
                 if not product_id:
                      log_error(f"Пропуск товара без ID: {product.get('name', 'N/A')}")
                      continue # Пропускаем товар без ID

                 # --- Получаем артикул (КРИТИЧЕСКИ ВАЖНО) ---
                 article = product.get("article", "") # Поле article в МойСклад
                 if not article:
                      # Если нет article, можно попробовать взять code
                      article = product.get("code", "")
                      if article:
                           # Логируем, что использовали code вместо article
                            print(f"  INFO: Для товара '{product.get('name')}' (ID: {product_id}) не найден 'article', используется 'code': {article}")
                      else:
                           # Если нет ни article, ни code - это проблема для Tilda
                           products_without_article += 1
                           log_error(f"КРИТИЧНО: Товар '{product.get('name')}' (ID: {product_id}) не имеет ни 'article', ни 'code'. Он не будет найден в Tilda!")
                           # Можно пропустить этот товар: continue
                           # Или добавить с пустым артикулом, но он не сработает в первом скрипте:
                           # article = "" # Оставляем пустым, скрипт Tilda его не найдет

                 # --- Получаем название товара ---
                 product_name = product.get("name", "Без названия")

                 # --- Получаем остатки из словаря ---
                 product_stores = stock_dict.get(product_id, [])
                 # Если остатков нет, можно добавить заглушку или оставить пустым
                 # if not product_stores:
                 #     product_stores = [{"store": "Нет данных", "quantity": 0}]

                 # --- Получаем категорию и родительскую категорию ---
                 tilda_category = None
                 tilda_parent_category = None
                 product_folder_data = product.get("productFolder") # Данные о папке товара

                 if product_folder_data and isinstance(product_folder_data, dict):
                      tilda_category = product_folder_data.get("name") # Имя папки товара

                      parent_folder_data = product_folder_data.get("parent") # Данные о родительской папке
                      if parent_folder_data and isinstance(parent_folder_data, dict):
                           tilda_parent_category = parent_folder_data.get("name") # Имя родительской папки

                 # --- Формируем объект для JSON ---
                 output_product = {
                      # Обязательные поля для скриптов Tilda:
                      "article": str(article), # Приводим к строке
                      "stores": product_stores,
                      "tilda_category": str(tilda_category) if tilda_category else None, # None станет null в JSON
                      "tilda_parent_category": str(tilda_parent_category) if tilda_parent_category else None,

                      # Дополнительные полезные поля:
                      "product_name": str(product_name),
                      # Можно добавить ID и Code если нужны для отладки
                      # "product_id_ms": product_id,
                      # "product_code_ms": product.get("code", ""),
                 }
                 result_list.append(output_product)

                 # Логирование прогресса каждые N товаров
                 if processed_count % 500 == 0:
                      print(f"  Обработано товаров: {processed_count}...")

            except Exception as e:
                 log_error(f"Критическая ошибка обработки товара ID {product.get('id', 'N/A')}: {e}")
                 continue # Продолжаем со следующим товаром

        print(f"Формирование JSON завершено. Товаров в итоговом списке: {len(result_list)}")
        if products_without_article > 0:
             print(f"  ВНИМАНИЕ: {products_without_article} товаров не имеют артикула/кода и не будут найдены в Tilda!")

        # 5. Сохранение результата в файл
        print(f"\nСохранение результата в файл: {OUTPUT_FILENAME}")
        try:
            with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
                # ensure_ascii=False - для сохранения кириллицы как есть
                # indent=2 или 4 - для читаемого форматирования (увеличивает размер файла)
                # indent=None - для компактного файла (меньше размер)
                json.dump(result_list, f, ensure_ascii=False, indent=2) # Используем indent=2 для читаемости
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
        # Ловим критические ошибки на верхнем уровне (например, если API недоступен)
        log_error(f"КРИТИЧЕСКАЯ ОШИБКА выполнения скрипта: {e}")
    finally:
        # input("\nНажмите Enter для выхода...") # Убрал, чтобы скрипт мог работать автоматически
        print("\nСкрипт завершил работу.")


if __name__ == "__main__":
    # Устанавливаем рабочую директорию на директорию скрипта
    # Это помогает, если скрипт запускается из другого места
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    print(f"Рабочая директория: {os.getcwd()}")

    generate_stock_json()
