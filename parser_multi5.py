import time
from bs4 import BeautifulSoup
from loguru import logger
import pandas as pd
import requests
import datetime
from threading import Thread

logger.add("file_{time}.log")

def send_msg(text):
    token = "token"
    chat_id = "chat_id"
    url_req = "https://api.telegram.org/bot" + token + "/sendMessage" + "?chat_id=" + chat_id + "&text=" + text
    results = requests.get(url_req)

def parsing_main(page):
    logger.info("Парсинг главной страницы начат")
    response = requests.get(page)
    if response.ok:
        html_doc = BeautifulSoup(response.text, features='html.parser')
        list_of_names = html_doc.find_all('a', class_='navigation__link')
        global category
        category = {}
        for names, href in zip(list_of_names, list_of_names):
            if names.text == "":
                logger.info('Парсинг главной страницы закончен')
            else:
                category[names.text] = "https://www.chitai-gorod.ru" + href.get('href')
    return response

def atributerror(book, metod, class_):
    try:
        atr = book.find(metod, {'class': class_}).text.strip()
    except AttributeError:
        atr = 'Нет'
    return atr

def publish(book):
    class_publish = {}
    for block in book.find_all('span', {'class': 'publisher'}):
        tag = block.find_all('span')
        class_publish[tag[0].text] = tag[1].text
    return class_publish

def keyerror(book, text):
    try:
        atr = publish(book)[text]
    except KeyError:
        atr = 'Нет'
    return atr

def pagination(html_doc):
    try:
        last_page = int([i.text.strip() for i in html_doc.find_all('a', {'class': 'pagination-item'})][-2])
    except:
        last_page = 1
    return last_page

def parsing_books_from_page(page):
    df1 = pd.DataFrame(columns=['Category', 'Name', 'Author', 'Price', 'Publisher', 'Year'])
    response = requests.get(category[name] + "?page=" + str(page))
    if response.ok:
        global html_doc
        html_doc = BeautifulSoup(response.text, features='html.parser')
        logger.debug('Парсинг категории {0}, страница {1}'.format(name, page))
        books = html_doc.find_all('div', {'class': 'product-card js_product js__product_card js__slider_item'})
        for book in books:
            kniga = book.find('div', {'class': 'product-card__title js-analytic-product-title'}).text.strip()
            author = atributerror(book, 'div', 'product-card__author')
            price = atributerror(book, 'span', 'product-price__value')
            publisher = keyerror(book, 'Издательство')
            year = keyerror(book, 'Год издания')
            df2 = pd.DataFrame([[name, kniga, author, price, publisher, year]],
                               columns=['Category', 'Name', 'Author', 'Price', 'Publisher', 'Year'])
            df1 = df1.append(df2)
        logger.info('Формирование датафрейма завершено')
        df1.to_csv('./books.csv', index=False, mode='a', header=None)
        logger.info('Датафрейм отправлен в таблицу CSV')
    else:
        logger.error('Страница {0} категории {1} недоступна'.format(page, name))

def clean_csv():
    with open('./books.csv', 'w'):
        pass

def allow(i, last, t):
    if i > last:
        pass
    else:
        t.start()
        # logger.debug('Поток {} стартовал'.format(t))
        time.sleep(0.2)
        # logger.debug('Поток {} выждал 0.2 секунды'.format(t))

def stop(t):
    if t.is_alive() == False:
        # logger.debug('Поток {} не запущен'.format(t))
        pass
    else:
        t.join()
        # logger.debug('Поток {} остановлен'.format(t))

###Основной код###
page = "https://www.chitai-gorod.ru/catalog/books/"
time_start = datetime.datetime.now()
send_msg("Парсинг запущен, время запуска - {0}".format(time_start))
clean_csv()

if parsing_main(page).ok:
    for name in category:
        parsing_books_from_page(1)
        last = pagination(html_doc)

        for i in range(2, last + 1, 5):
            t = [Thread(target=parsing_books_from_page, name=f'Tread{i + r}', args=(i + r,)) for r in range(5)]
            lst = ['1', '2', '3', '4', '5']
            for j in range(5):
                vars()["t" + lst[j]] = t[j]

            f = [t1, t2, t3, t4, t5]
            for r in range(5):
                allow(i + r, last, f[r])

            for t in f:
                stop(t)

        send_msg("{0} - Закончена категория {1}".format(datetime.datetime.now(), name))
else:
    logger.error('Главная страница недоступна')

logger.debug('Чтение файла из CSV')
df3 = pd.read_csv('./books.csv', names=['Category', 'Name', 'Author', 'Price', 'Publisher', 'Year'])
logger.debug('Конвертация')
df3.to_excel('./books.xlsx', index=False)
logger.info('Успешно')

time_end = datetime.datetime.now()
diff = time_end - time_start
send_msg("Парсинг завершен, время окончания - {0}, длительность работы парсера - {1} секунд".format(time_end,
                                                                                                    diff.total_seconds()))