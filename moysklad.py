import requests
import json

# Функция для получения полной иерархии категорий
def get_category_hierarchy(category_id, categories_dict):
    path = []
    while category_id:
        category = categories_dict.get(str(category_id))
        if not category:
            break
        path.append(category["name"])
        category_id = category.get("parent")
    return " > ".join(reversed(path))

# Запрос данных из Мой Склад
API_URL = "https://api.moysklad.ru/api/remap/1.2/entity/product"
HEADERS = {"Authorization": "Bearer a88e8da42807ebf8f89e6fdef605193f7a9ddc8c", "Content-Type": "application/json"}
response = requests.get(API_URL, headers=HEADERS)
data = response.json()

# Получаем список категорий
categories_url = "https://api.moysklad.ru/api/remap/1.2/entity/productfolder"
categories_response = requests.get(categories_url, headers=HEADERS)
categories_data = categories_response.json().get("rows", [])

# Создаём словарь категорий
categories_dict = {category["id"]: {"name": category["name"], "parent": category.get("productFolder") and category["productFolder"].get("id")} for category in categories_data}

# Обрабатываем товары
products = []
for item in data.get("rows", []):
    category_id = item.get("productFolder") and item["productFolder"].get("id")
    full_category_path = get_category_hierarchy(category_id, categories_dict) if category_id else None
    
    product = {
        "id": item["id"],
        "name": item["name"],
        "code": item.get("code"),
        "article": item.get("article"),
        "tilda_category": categories_dict.get(str(category_id), {}).get("name"),
        "tilda_parent_category": full_category_path,
        "stores": [
            {"store": store["name"], "quantity": store.get("stock", 0)}
            for store in item.get("storeStock", [])
        ]
    }
    products.append(product)

# Сохраняем данные в JSON
with open("stock_data.json", "w", encoding="utf-8") as f:
    json.dump(products, f, ensure_ascii=False, indent=4)

print("Данные успешно обновлены и сохранены в stock_data.json")
