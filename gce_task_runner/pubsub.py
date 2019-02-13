import logging
import sys
import time
from contextlib import contextmanager

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub

logger = logging.getLogger(__name__)


class PublishClient:
    """Publisherのラッパークラス."""

    def __init__(self, project):  # noqa: D107
        self.project = project
        self.service = pubsub.PublisherClient()

    def publish(self, topic, data, **kwargs):
        """通知イベントの発行."""
        topic_path = self.service.topic_path(self.project, topic)
        data = data.encode('utf-8')
        self.service.publish(topic_path, data, **kwargs)


class SubscribeClient:
    """Subscriberのラッパークラス."""

    def __init__(self, project):  # noqa: D107
        self.project = project
        self.service = pubsub.SubscriberClient()

    def subscribe_async(self, subscription, callback):
        """通知の購読(非同期)."""
        path = self.service.subscription_path(self.project, subscription)
        return self.service.subscribe(path, callback)

    def subscribe(self, subscription, callback, check, sleep=1):
        """通知の購読."""
        def _callback(message):
            r = callback(message)
            message.ack()
            return r

        future = self.subscribe_async(subscription, _callback)
        try:
            for spinner in _spinner():
                sys.stdout.write(spinner)
                sys.stdout.flush()
                time.sleep(sleep)
                sys.stdout.write('\b')
                if not check():
                    break
        finally:
            future.cancel()

    def delete_subscription(self, subscription):
        """サブスクリプションの削除."""
        path = self.service.subscription_path(self.project, subscription)
        self.service.delete_subscription(path)
        logger.info('delete: {}'.format(path))

    def create_subscription(self, topic, subscription):
        """新しいサブスクリプションの作成."""
        path = self.service.subscription_path(self.project, subscription)
        topic_path = self.service.topic_path(self.project, topic)
        try:
            self.service.create_subscription(path, topic_path)
        except AlreadyExists:
            pass
        logger.info('create: {}'.format(path))


@contextmanager
def context(project, topic, subscription):
    """PubSubサブスクリプションのコンテキスト."""
    client = SubscribeClient(project)
    client.create_subscription(topic, subscription)
    try:
        yield client
    finally:
        client.delete_subscription(subscription)


def _spinner():
    while True:
        yield '|'
        yield '/'
        yield '-'
        yield '\\'
