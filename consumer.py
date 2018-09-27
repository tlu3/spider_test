from funcs import get_house_info, get_soup
from threading import Thread
import db
from const import QUEUE_NAME, RABBITMQ_PORT, RABBITMQ_HOST
import pika
import logging


# 创建消费者类
class Consumer(Thread):

    def run(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat=0))
        channel = connection.channel()

        # define queue
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        def handler(url):
            logging.debug('getting house infos on url : {url}'.format(url=url))
            print('get url :----------------------------', url)
            soup = get_soup(url)
            houses = soup.find_all('div', {'class': 'nlc_details'})
            for house in houses:
                house_info = get_house_info(house)
                db.insert_into_db(house_info)

        # 定义接收到消息的处理方法
        def request(ch, method, properties, body):
            url = body

            handler(url)

        # ch.basic_ack(delivery_tag=method.delivery_tag)

        # channel.basic_qos(prefetch_count=1)
        channel.basic_consume(request, queue=QUEUE_NAME, no_ack=True)

        channel.start_consuming()


def start_consuming():
    for i in range(5):
        c = Consumer()
        c.start()


if __name__ == '__main__':
    start_consuming()
