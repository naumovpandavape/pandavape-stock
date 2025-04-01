import requests
import json

API_URL = "https://api.moysklad.ru/api/remap/1.2/entity/product"
API_TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def get_categories():
    url = "https://api.moysklad.ru/api/remap/1.2/entity/productfolder"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        categories = response.json().get("rows", [])
        category_dict = {cat["id"]: cat for cat in categories}
        return category_dict
    return {}

def get_category_hierarchy(category_id, categories_dict):
    hierarchy = []
    while category_id:
        category = categories_dict.get(category_id)
        if not category:
            break
        hierarchy.insert(0, category["name"])
        category_id = category.get("productFolder", {}).get("id")
    return hierarchy

def get_products():
    response = requests.get(API_URL, headers=HEADERS)
    if response.status_code == 200:
        return response.json().get("rows", [])
    return []

def get_stocks():
    stock_url = "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
    response = requests.get(stock_url, headers=HEADERS)
    if response.status_code == 200:
        return {item["id"]: item.get("stock", 0) for item in response.json().get("rows", [])}
    return {}

def main():
    categories_dict = get_categories()
    products = get_products()
    stocks = get_stocks()

    result = []
    for product in products:
        category_id = product.get("productFolder", {}).get("id")
        category_hierarchy = get_category_hierarchy(category_id, categories_dict)
        tilda_category = category_hierarchy[-1] if category_hierarchy else None
        tilda_parent_category = category_hierarchy[-2] if len(category_hierarchy) > 1 else None

        product_data = {
            "id": product["id"],
            "name": product["name"],
            "code": product.get("code", ""),
            "article": product.get("article", ""),
            "tilda_category": tilda_category,
            "tilda_parent_category": tilda_parent_category,
            "stores": [
                {"store": store, "quantity": stocks.get(product["id"], 0)}
                for store in ["9 Января 49", "Бульвар Победы 38 (Линия)", "Ленинский проспект 116Е"]
            ]
        }
        result.append(product_data)

    with open("stock_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()
