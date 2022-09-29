import json
from enum import auto

import pika
import requests
import sys
from os import getenv


def handle_message(ch, method, properties, body):
    link_json = json.loads(body)
    get = requests.get(link_json["url"])
    url = f'http://nginx:80/link/{link_json["id"]}'
    request_body = {"status": get.status_code}
    requests_put = requests.put(url, json=request_body, timeout=10)
    requests_put.raise_for_status()


def main():
    user = getenv("RABBITMQ_DEFAULT_USER")
    password = getenv("RABBITMQ_DEFAULT_PASS")
    hostname = getenv("RABBIT_HOSTNAME")
    connection = pika.BlockingConnection(pika.URLParameters(f"amqp://{user}:{password}@{hostname}:5672/%2F"))
    channel = connection.channel()

    QUEUE_NAME = 'links'
    channel.queue_declare(queue=QUEUE_NAME)

    channel.basic_consume(queue=QUEUE_NAME, auto_ack=True, on_message_callback=handle_message)
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
