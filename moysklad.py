import requests
import json

def get_category_hierarchy(category_id, categories_dict):
    """
    Рекурсивно получает всю иерархию категорий.
    """
    category = categories_dict.get(category_id)
    if category:
        parent_category_id = category.get("parentId")
        if parent_category_id:
            parent_category = get_category_hierarchy(parent_category_id, categories_dict)
            return f"{parent_category} > {category['name']}"
        return category["name"]
    return None

# Запрос данных из Моего Склада
TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Получаем все категории товаров
categories_url = "https://api.moysklad.ru/api/remap/1.2/entity/productfolder"
categories_response = requests.get(categories_url, headers=HEADERS)
categories_data = categories_response.json().get("rows", [])

# Создаём словарь для быстрого поиска категорий по id
categories_dict = {category["id"]: category for category in categories_data}

# Получаем все товары
products_url = "https://api.moysklad.ru/api/remap/1.2/entity/product"
products_response = requests.get(products_url, headers=HEADERS)
products_data = products_response.json().get("rows", [])

# Получаем данные о складах
stores_url = "https://api.moysklad.ru/api/remap/1.2/entity/store"
stores_response = requests.get(stores_url, headers=HEADERS)
stores_data = {store["id"]: store["name"] for store in stores_response.json().get("rows", [])}

# Получаем остатки товаров на складах
stock_url = "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
stock_response = requests.get(stock_url, headers=HEADERS)
stock_data = stock_response.json().get("rows", [])

# Обрабатываем данные
products_list = []
for product in products_data:
    product_id = product["id"]
    category_id = product.get("productFolder")
    full_category_path = get_category_hierarchy(category_id, categories_dict)
    
    # Получаем остатки по складам
    product_stocks = [
        {"store": stores_data.get(stock["storeId"], "Unknown"), "quantity": stock["quantity"]}
        for stock in stock_data if stock["assortmentId"] == product_id
    ]
    
    # Формируем JSON-объект
    product_entry = {
        "id": product_id,
        "name": product["name"],
        "code": product.get("code", ""),
        "article": product.get("article", ""),
        "tilda_category": full_category_path.split(" > ")[-1] if full_category_path else None,
        "tilda_parent_category": " > ".join(full_category_path.split(" > ")[:-1]) if full_category_path else None,
        "stores": product_stocks
    }
    products_list.append(product_entry)

# Сохраняем в JSON
with open("stock_data.json", "w", encoding="utf-8") as f:
    json.dump(products_list, f, ensure_ascii=False, indent=4)

print("Данные успешно собраны и сохранены в stock_data.json")
