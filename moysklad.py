import requests
import json

API_URL = "https://online.moysklad.ru/api/remap/1.2/entity/product"
TOKEN = "a88e8da42807ebf8f89e6fdef605193f7a9ddc8c"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

def get_category_hierarchy(category_id, categories_dict):
    hierarchy = []
    while category_id:
        category = categories_dict.get(category_id)
        if not category:
            break
        hierarchy.append(category["name"])
        category_id = category.get("parent")
    return " > ".join(reversed(hierarchy)) if hierarchy else None

def fetch_categories():
    url = "https://online.moysklad.ru/api/remap/1.2/entity/productfolder"
    response = requests.get(url, headers=HEADERS)
    categories = response.json().get("rows", [])
    return {cat["id"]: {"name": cat["name"], "parent": cat.get("productFolder", {}).get("id")} for cat in categories}

def fetch_products():
    response = requests.get(API_URL, headers=HEADERS)
    return response.json().get("rows", [])

def fetch_stock():
    stock_url = "https://online.moysklad.ru/api/remap/1.2/report/stock/all"
    response = requests.get(stock_url, headers=HEADERS)
    return response.json().get("rows", [])

def main():
    categories_dict = fetch_categories()
    products = fetch_products()
    stock_data = fetch_stock()
    stock_dict = {item["id"]: item["stock"] for item in stock_data}
    
    result = []
    for product in products:
        category_id = product.get("productFolder", {}).get("id")
        full_category_path = get_category_hierarchy(category_id, categories_dict) if category_id else None
        
        item = {
            "id": product["id"],
            "name": product["name"],
            "code": product.get("code"),
            "article": product.get("article"),
            "tilda_category": full_category_path,
            "tilda_parent_category": categories_dict.get(category_id, {}).get("parent"),
            "stores": [
                {"store": stock["name"], "quantity": stock_dict.get(product["id"], 0)}
                for stock in stock_data if stock.get("assortment", {}).get("id") == product["id"]
            ]
        }
        result.append(item)
    
    with open("stock_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

if name == "__main__":
    main()
