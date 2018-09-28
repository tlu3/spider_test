import logging
from threading import Thread

from broker import send_url_to_queue, setup_connection, close_connection, setup_queue
from const import BASE_URL, QUEUE_NAME
from funcs import get_urls, get_regions


# 创建生产者类
class Producer(Thread):

    def __init__(self, url):
        Thread.__init__(self)
        self.url = url

    def run(self):
        connection = setup_connection()
        channel = setup_queue(connection, QUEUE_NAME)
        logging.debug('crawling urls')
        urls = get_urls(self.url)
        logging.debug('get urls: {urls}'.format(urls=str(urls)))
        for url in urls:
            logging.debug('sending url {url} to queue {queue}'.format(url=url, queue=QUEUE_NAME))
            send_url_to_queue(channel, url, QUEUE_NAME)
        close_connection(connection)


def start_producing():
    # get regions
    regions = get_regions()
    for region, url in regions.items():
        logging.debug('crawling region {region}'.format(region=region))
        print('-------crawling region : %s--------' % region)
        url = BASE_URL + url
        p = Producer(url)
        p.start()


if __name__ == '__main__':
    start_producing()
