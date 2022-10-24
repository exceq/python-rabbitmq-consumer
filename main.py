import json
from typing import Optional

import pika
import redis as redis
import requests
import sys
from os import getenv

cache = redis.Redis(host=getenv("CACHE_HOST"))
app_hostname = getenv("APP_HOSTNAME")


def fetch_status_from_internet(url: str) -> int:
    get = requests.get(url)
    return get.status_code


def get_from_cache(cache_key: str) -> Optional[int]:
    value = cache.get(cache_key)
    return value.decode('utf-8') if value else None


def set_cache(cache_key: str, status_code: int) -> None:
    cache.set(cache_key, status_code, ex=60)


def get_status(url: str) -> int:
    cache_key = f"url-{url}"
    status_code = get_from_cache(cache_key)
    print("Status code of cached url:", url, status_code)
    if status_code is None:
        status_code = fetch_status_from_internet(url)
        set_cache(cache_key, status_code)

    return status_code


def handle_message(ch, method, properties, body):
    link_json = json.loads(body)
    url = f'http://{app_hostname}/link/{link_json["id"]}'
    request_body = {"status": get_status(link_json["url"])}
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
