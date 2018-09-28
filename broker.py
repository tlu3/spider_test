import logging

import pika

from const import RABBITMQ_HOST, RABBITMQ_PORT


def send_url_to_queue(channel, body, queue_name):
    channel.basic_publish(exchange='', routing_key=queue_name, body=str(body),
                          properties=pika.BasicProperties(delivery_mode=2))  # make message persistent
    # default: routing_key --> queue name, body--> msg body
    logging.debug('send msg: {body}'.format(body=str(body)))
    print('send msg : %s' % body)


def get_url_from_queue(channel, queue_name, handler=None):
    # 定义接收到消息的处理方法
    def request(ch, method, properties, body):
        url = body
        print('got msg : %s' % body)
        logging.debug('getting url {url} from queue {queue}'.format(url=url, queue=queue_name))
        if callable(handler):
            handler(url)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(request, queue=queue_name)

    channel.start_consuming()


def setup_connection():
    logging.debug('setting up connection to {host}:{port}'.format(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT))
    return connection


def setup_queue(connection, queue_name):
    logging.debug('defining queue {queue}'.format(queue=queue_name))
    channel = connection.channel()
    # define queue
    channel.queue_declare(queue=queue_name, durable=True)
    return channel


def close_connection(connection):
    logging.debug('closing connection')
    connection.close()
