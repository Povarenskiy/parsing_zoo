from bs4 import BeautifulSoup as BS
import pandas as pd
import time
import random
import requests
import datetime
import json
import re
import logging
import csv
import os
from logging import FileHandler, Formatter


def get_product(data, url, category):
    """
    Функция парсинга данных о товаре на параметры
    Для каждого параметра при парсинге существует проверка, при ошибке записывается None
    На вход функция получает data в виде класса bs4.BeautifulSoup, url продукта и категорию
    """
    # Параметр: время и дата запроса
    price_datetime = datetime.datetime.now()

    # Параметр: категория
    sku_category = category

    # Параметр: ссылка
    sku_link = url

    # Параметр: название товара
    try:
        sku_name = data.find("title").text
    except:
        sku_name = None

    # Параметр: страна производитель
    try:
        sku_country = data.find("div", class_="catalog-element-offer-left").text.strip().split(":")[-1]
    except:
        sku_country = None

    # Параметр: ссылки на все картинки
    sku_images = []
    try:
        images = data.find("div", class_="catalog-element-pictures").find_all("a")
        for ref in images:
            sku_images.append(domain + ref.get("href"))
    except:
        sku_images = None

    # Для продуктов существует множество вариантов фасовок со уникальными штрихкодами и артикулами
    try:
        items = data.find("div", class_="catalog-element-offer active").find_all("tr", class_="b-catalog-element-offer")
    except:
        items = [None]

    for item in items:

        # Параметр: регулярная цена
        try:
            price = item.find(text="Цена:").find_next("s").text
        except:
            price = None

        # Параметр: акционная цена
        try:
            price_promo = item.find("span", class_="catalog-price").text
        except:
            price_promo = None

        # Параметр: наличие товара
        try:
            if item.find("div", class_="catalog-item-no-stock") is None:
                sku_status = 1
            else:
                sku_status = 0
        except:
            sku_status = None

        # Параметр: штрихкод
        try:
            sku_barcode = item.find(text="Штрихкод:").find_next("b").text
        except:
            sku_barcode = None

        # Параметр: артикль
        try:
            sku_article = item.find(text="Артикул:").find_next("b").text
        except:
            sku_article = None

        # смотрим фасовку товара, это будет либо вес/ либо объем/ либо количество единиц
        try:
            packing = item.find(text="Фасовка:").find_next("b").text
        except:
            packing = None

        # по умолчанию все параметры None
        sku_weight_min = None
        sku_volume_min = None
        sku_quantity_min = None

        # По размерности, укзанной в фасовке, определяется тип: вес, объем, количество
        if packing is not None:
            dimension = ''.join(re.findall('[а-я]', packing))
            if dimension in ['г', 'гр', 'кг']:
                sku_weight_min = packing
            if dimension in ['мл', 'л']:
                sku_volume_min = packing
            if dimension in ['шт']:
                sku_quantity_min = packing

    # запись результатов парсинга
    result = {"price_datetime": price_datetime,
            "price": price,
            "price_promo": price_promo,
            "sku_status": sku_status,
            "sku_barcode": sku_barcode,
            "sku_article": sku_article,
            "sku_name": sku_name,
            "sku_category": sku_category,
            "sku_country": sku_country,
            "sku_weight_min": sku_weight_min,
            "sku_volume_min": sku_volume_min,
            "sku_quantity_min": sku_quantity_min,
            "sku_link": sku_link,
            "sku_images": sku_images}

    return result


def try_request(url, headers, max_count, delay_min, delay_max):
    """
    Функция получения данных страницы категории и товаров
    Учитывается максимальное количетсво попыток запроса
    Задержка между запросами определяется случайным образом в указанном диапазоне
    """
    retry_count = 0
    while retry_count < max_count:
        try:
            time.sleep(random.randint(delay_min, delay_max))
            r = requests.get(url, headers=headers, timeout=10)
            return BS(r.content, "html.parser")
        except:
            retry_count += 1
            logger.info(f'Попытка {retry_count}/{max_retries} подключиться к {url}')
    return 0

def check_and_write(data, dir):
    """
    Функция сопоставления штрихкода и артикля с уже записанными продуктами
    Если проверка не выявила повторов, происходит запись в файл
    """

    # заголовки для файла
    fieldnames = ["price_datetime", "price", "price_promo", "sku_status",
                  "sku_barcode", "sku_article", "sku_name", "sku_category",
                  "sku_country", "sku_weight_min", "sku_volume_min",
                  "sku_quantity_min", "sku_link", "sku_images"]

    # проверяем на вопторвы, если файла не существует, создаем и записываем заголовки
    check = True
    try:
        with open(f'{dir}/result.csv', 'r', encoding="utf8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if (data['sku_barcode'] == row['sku_barcode']) and (
                        data['sku_barcode'] == row['sku_barcode']):
                    check = False
    except:
        with open(f'out/result.csv', 'a', newline='', encoding="utf8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    # если проверка пройдена осуществляем запись
    if check:
        with open(f'out/result.csv', 'a', newline='', encoding="utf8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(product_result)


if __name__ == "__main__":

    # результат парсера записывается в список
    products_book = []
    # указывается домен
    domain = "https://zootovary.ru"

    # считывается список категорий
    categories = pd.read_csv("categories.csv", encoding="UTF-8", sep=";")

    # считываение и настройка параметров с файла config.json
    with open('config.json') as f:
        config = json.load(f)

    out_dir = config['output_directory']
    if out_dir is None:
        out_dir = 'out'
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)


    delay = config['delay_range_s']
    if delay == 0:
        delay_min = delay_max = 0
    else:
        delay_min = delay['min']
        delay_max = delay['max']

    # парсер происходит по категориям, если категории не указаны, выбираются все категории
    select_list = config['categories']
    if select_list is None:
        select_list = categories['id'].to_list()

    max_retries = config['max_retries']
    max_restart_count = config['restart']['restart_count']
    sleep_interval = config['restart']['interval_m'] * 60

    headers = config['headers']
    logs_dir = config['logs_dir']

    # настройка логов
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    handler = FileHandler(f'{logs_dir}/log.txt')
    handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
    logger.addHandler(handler)

    # основной цикл программы, учитывается количество попыток парсера
    # Повторы записываются и удаляются в конце программы
    restart_count = 0
    while restart_count < max_restart_count:
        try:
            for id_ in select_list:
                URL = categories.at[id_, "url"]
                product_category = categories.at[id_, "category"]

                # запрос к стрницам категории, получаем перечисленные продукты
                page = 1
                page_max = 1
                while page <= page_max:

                    page_url = URL + f'?PAGEN_1={page}'
                    # вывод в консоль
                    print(f'Обработка страницы: "{page}", категории: "{product_category}"')
                    page_data = try_request(page_url, headers, max_retries, delay_min, delay_max)

                    if page_data:
                        if page_max == 1:
                            try:
                                page_max = page_data.find("div", class_="navigation").find_all("a")[-1].get("href")
                                page_max = int(page_max.split("=")[-1])
                            except:
                                page_max = 1

                        items = page_data.find_all("div", class_="catalog-item")

                        # в продуктах получаем ссылку на продукт, делаем запрос, парсим данныы
                        for item in items:

                            product_url = domain + item.find("a", class_="name").get("href")
                            print(f'Обработка продукта категории: "{product_category}" по адресу: "{product_url}"')
                            product_data = try_request(product_url, headers, max_retries, delay_min, delay_max)

                            if product_data:
                                product_result = get_product(product_data, product_url, product_category)
                                check_and_write(product_result, out_dir)

                            else:
                                logger.info(f'Ошибка загрузки товара : "{product_category}"')

                        page += 1

                    else:
                        logger.info(f'Загрузка страницы: "{page}", категории: "{product_category}" не удалась')
                        break
            # Выход из цикла при успешной записи
            logger.info(f'Попытка парсера № {restart_count} удалась!')
            break
        except:
            # Рестарт при аварийном завершении парсинга
            restart_count += 1
            logger.info(f'Попытка парсера № {restart_count} не удалась, рестарт через {sleep_interval}')
            time.sleep(sleep_interval)


