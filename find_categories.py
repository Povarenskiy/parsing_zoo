from bs4 import BeautifulSoup as BS
import requests
import pandas as pd

"""
Данный модуль разработан для получения общего списка категорий с сайта
На данном этапе не производятся проверки запросов и считываение файла config.json, которые
реализованы в осномном модуле с парсингом main
"""

# результат парсера записывается в список
categories = []

# указывается домен
domain = "https://zootovary.ru"

# запрос
r = requests.get(domain)
html = BS(r.content, "html.parser")

# главные категории продуктов
items = html.find_all("li", class_="lev1")

parent_id = 0
for item in items:

    # список подкатегорий
    sub_categories = item.find("li", class_="col1").find_all("a")

    # парсинг подкатегории
    item_name = item.find("a").get("title")
    item_url = domain + item.find("a").get("href")
    sub_id = parent_id

    for sub_item in sub_categories:
        sub_url = domain + sub_item.get("href")
        sub_name = sub_item.text
        sub_id += 1

        # добавление подкатегории
        categories.append(
            {
                "name": sub_name,
                "id": sub_id,
                "parent_id": parent_id,
                "category": f'{item_name}|{sub_name}',
                "url": sub_url
            })

    # добавление главной категории
    categories.append(
        {
            "name": item_name,
            "id": parent_id,
            "parent_id": None,
            "category": item_name,
            "url": item_url
        })
    parent_id = sub_id + 1

# запись списка категорий в датафрейм, сортировка
db = pd.DataFrame(categories).sort_values("id")
db.set_index("name", inplace=True)

# запись в файл
db.to_csv("categories.csv", encoding="UTF-8", sep=";")
