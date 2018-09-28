from functools import wraps
import logging
import re
import time

from bs4 import BeautifulSoup
import requests

from const import BASE_URL


# test performance decorator
def time_consuming(func):
    @wraps(func)
    def wrapper(*args):
        start_time = time.time()
        result = func(*args)
        end_time = time.time()
        logging.debug('func:{func}, consuming time: {time}'.format(func=func.__name__, time=end_time - start_time))
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
        logging.debug('getting url for region : {region}'.format(region=region))
        district = region.get_text()
        url = region['href']
        logging.debug('got region : {region}, with url : {url}'.format(region=region, url=url))
        regions[district] = url

    return regions


# get urls of all related pages
@time_consuming
def get_urls(url):
    urls = list()
    soup = get_soup(url)
    page_data = soup.find('div', {'class': 'page'})
    if page_data:
        last_url = page_data.find('a', {'class': 'last'})['href'].strip()
        last_page = int(last_url.split('/')[-2][2:])
        prefix_url = '/'.join(last_url.split('/')[:-2])
        postfix_last_page = last_url.split('/')[-2][:2]
        for num in range(1, last_page + 1):
            new_url = BASE_URL + prefix_url + '/' + postfix_last_page + str(num)
            urls.append(new_url)
    else:
        urls.append(url)
    return urls


def get_houses_info(url):
    logging.debug('getting houses infos on url : {url}'.format(url=url))
    soup = get_soup(url)
    houses = soup.find_all('div', {'class': 'nlc_details'})
    houses_info = list()
    for house in houses:
        logging.debug('getting house {house} info'.format(house=str(house)))
        house_info = get_house_info(house)
        houses_info.append(house_info)
    return houses_info


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
    logging.debug('got house {house} with title: {title}'.format(house=str(house), title=title))
    return title


def get_house_url(house):
    url = house.find('a', {'target': '_blank'})['href']
    logging.debug('got house {house} with url: {url}'.format(house=str(house), url=url))

    return url


def get_house_style(house):
    style = house.find('div', {'class': 'house_type'}).get_text().replace('\t', '').replace('\n', '')
    logging.debug('got house {house} with style: {style}'.format(house=str(house), style=style))

    return style


def get_house_addr(house):
    addr = house.find('div', {'class': 'address'}).find('a')['title']
    logging.debug('got house {house} with address: {addr}'.format(house=str(house), addr=addr))

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
    logging.debug('got house {house} with price: {price}'.format(house=str(house), price=price))

    return price
