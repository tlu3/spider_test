from bs4 import BeautifulSoup
import requests
import re
import time
from functools import wraps
import queue
from const import BASE_URL
import pika


# 创建一个队列
q = queue.Queue()


# test performance decorator
def time_consuming(func):
    @wraps(func)
    def wrapper(*args):
        start_time = time.time()
        result = func(*args)
        end_time = time.time()
        print('---func:%s, consuming time: %s s---' % (func.__name__, end_time - start_time))
        return result

    return wrapper


# get a soup instance
@time_consuming
def get_soup(url):
    html = requests.get(url)
    html.encoding = 'gbk'
    soup = BeautifulSoup(html.text, 'html.parser')
    return soup


# get each district and corresponding url
@time_consuming
def get_regions():
    soup = get_soup(BASE_URL)
    regions_data = soup.find('li', {'class': 'quyu_name'}).find_all('a', {'href': re.compile('/house/s/.*?/')})
    regions = dict()
    for region in regions_data:
        district = region.get_text()
        url = region['href']
        regions[district] = url

    return regions


# get urls of all related pages
@time_consuming
def get_urls(url, channel, queue_name):
    soup = get_soup(url)
    page_data = soup.find('div', {'class': 'page'})
    if page_data:
        last_url = page_data.find('a', {'class': 'last'})['href'].strip()
        last_page = int(last_url.split('/')[-2][2:])
        prefix_url = '/'.join(last_url.split('/')[:-2])
        postfix_last_page = last_url.split('/')[-2][:2]

        for num in range(1, last_page + 1):
            new_url = BASE_URL + prefix_url + '/' + postfix_last_page + str(num)
            send_msg(channel, new_url, queue_name)

    else:
        send_msg(channel, url, queue_name)


# rabbitMQ send message through exchange
def send_msg(channel, body, queue_name):

    channel.basic_publish(exchange='', routing_key=queue_name, body=str(body),
                          properties=pika.BasicProperties(delivery_mode=2))  # make message persistent
    # default: routing_key --> queue name, body--> msg body

    print('send msg : %s' % body)


# get house info, including name style address and price
@time_consuming
def get_house_info(house):
    info = dict()
    title = get_house_title(house)
    style = get_house_style(house)
    addr = get_house_addr(house)
    price = get_house_price(house)
    house_url = get_house_url(house)
    info['style'] = style
    info['url'] = house_url
    info['addr'] = addr
    info['price'] = price
    info['title'] = title
    return info


def get_house_title(house):
    title = house.find('a', {'target': '_blank'}).get_text().strip()
    return title


def get_house_url(house):
    url = house.find('a', {'target': '_blank'})['href']
    return url


def get_house_style(house):
    style = house.find('div', {'class': 'house_type'}).get_text().replace('\t', '').replace('\n', '')
    return style


def get_house_addr(house):
    addr = house.find('div', {'class': 'address'}).find('a')['title']
    return addr


def get_house_price(house):
    price_data = house.find('div', {'class': 'nhouse_price'})
    price = ''
    if price_data:
        if price_data.find('span'):
            price = price_data.get_text().strip()
        elif price_data.find('i'):
            price = price_data.find('i').get_text().strip()
            unit = price_data.find('em').get_text().strip()
            price += unit
    return price
