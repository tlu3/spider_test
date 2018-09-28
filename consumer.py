from threading import Thread

from broker import setup_queue, setup_connection, get_url_from_queue
from const import QUEUE_NAME
from db import insert_into_db
from funcs import get_houses_info


# 创建消费者类
class Consumer(Thread):

    def run(self):
        connection = setup_connection()
        channel = setup_queue(connection, QUEUE_NAME)

        def handler(url):
            houses_info = get_houses_info(url)
            insert_into_db(houses_info)

        get_url_from_queue(channel, QUEUE_NAME, handler)


def start_consuming():
    for i in range(5):
        c = Consumer()
        c.start()


if __name__ == '__main__':
    start_consuming()
