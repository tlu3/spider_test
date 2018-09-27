from funcs import get_urls, get_regions
from const import BASE_URL, QUEUE_NAME,RABBITMQ_PORT,RABBITMQ_HOST
from threading import Thread
import pika
import logging


# 创建生产者类
class Producer(Thread):

    def __init__(self, url):
        Thread.__init__(self)
        self.url = url

    def run(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT,heartbeat=0))
        channel = connection.channel()

        # define queue
        channel.queue_declare(queue=QUEUE_NAME,durable=True)

        get_urls(self.url, channel, QUEUE_NAME)
        connection.close()


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