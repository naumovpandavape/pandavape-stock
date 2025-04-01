import requests
import json

# Токен авторизации (скрой его в переменных окружения!)
TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
BASE_URL = "https://api.moysklad.ru/api/remap/1.2/entity/"

# Получаем категории товаров

def get_categories():
    url = f"{BASE_URL}productfolder"
    response = requests.get(url, headers=HEADERS).json()
    return {item["id"]: {"name": item["name"], "parent": item.get("productFolder", {}).get("id")} for item in response.get("rows", [])}

# Рекурсивно собираем иерархию категорий

def get_category_hierarchy(category_id, categories):
    path = []
    while category_id:
        category = categories.get(category_id)
        if not category:
            break
        path.append(category["name"])
        category_id = category["parent"]
    return " > ".join(reversed(path))

# Получаем остатки товаров

def get_stocks():
    url = f"{BASE_URL}report/stock/all"
    response = requests.get(url, headers=HEADERS).json()
    return {item.get("assortment", {}).get("id"): item.get("stock", 0) for item in response.get("rows", []) if "assortment" in item}

# Получаем список товаров

def get_products():
    url = f"{BASE_URL}product"
    response = requests.get(url, headers=HEADERS).json()
    return response.get("rows", [])

# Основной процесс сбора данных

def main():
    categories = get_categories()
    stocks = get_stocks()
    products = get_products()
    
    result = []
    for product in products:
        category_id = product.get("productFolder", {}).get("id")
        full_category_path = get_category_hierarchy(category_id, categories)
        
        result.append({
            "id": product["id"],
            "name": product["name"],
            "code": product.get("code"),
            "article": product.get("article"),
            "tilda_category": full_category_path.split(" > ")[-1] if full_category_path else None,
            "tilda_parent_category": " > ".join(full_category_path.split(" > ")[:-1]) if full_category_path else None,
            "stores": [{"store": store_name, "quantity": stocks.get(product["id"], 0)} for store_name in stocks]
        })
    
    with open("stock_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    main()
