import requests
import json

# Настройки
API_URL = "https://api.moysklad.ru/api/remap/1.2"
API_TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Encoding": "gzip"
}

# Получаем ВСЕ товары (с пагинацией)
def get_all_assortment():
    url = f"{API_URL}/entity/assortment"
    params = {"limit": 100}  # Максимальный лимит
    all_products = []
    
    while url:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            all_products.extend(data.get("rows", []))
            url = data.get("meta", {}).get("nextHref")  # Следующая страница
            params = None  # Для последующих запросов URL уже содержит параметры
        else:
            print(f"Ошибка при запросе товаров: {response.text}")
            break
    return all_products

# Получаем ВСЕ остатки (с пагинацией)
def get_all_stock():
    url = f"{API_URL}/report/stock/bystore"
    params = {"limit": 100}
    all_stock = []
    
    while url:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            all_stock.extend(data.get("rows", []))
            url = data.get("meta", {}).get("nextHref")
            params = None
        else:
            print(f"Ошибка при запросе остатков: {response.text}")
            break
    return all_stock

# Формируем JSON
def generate_stock_json():
    assortment = get_all_assortment()
    stock_data = get_all_stock()

    result = []
    for product in assortment:
        product_id = product.get("id")
        product_name = product.get("name")
        product_code = product.get("code")
        product_article = product.get("article")
        stores = []

        # Ищем остатки для товара
        for stock in stock_data:
            stock_product_id = stock.get("meta", {}).get("href", "").split("/")[-1].split("?")[0]
            if stock_product_id == product_id:
                stock_by_store = stock.get("stockByStore", [])
                for store in stock_by_store:
                    store_name = store.get("name")
                    quantity = store.get("stock", 0)
                    if store_name:
                        stores.append({"store": store_name, "quantity": quantity})

        result.append({
            "id": product_id,
            "name": product_name,
            "code": product_code,
            "article": product_article,
            "stores": stores if stores else [{"store": "Нет данных", "quantity": 0}]
        })

    # Сохраняем JSON
    with open("stock_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    
    print(f"Успешно обработано товаров: {len(result)} из {len(assortment)}")

generate_stock_json()