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

    def create_topic(self, topic):
        topic_path = self.service.topic_path(self.project, topic)
        try:
            self.service.create_topic(topic_path)
        except AlreadyExists:
            pass
        return topic_path

    def delete_topic(self, topic):
        topic_path = self.service.topic_path(self.project, topic)
        try:
            self.service.delete_topic(topic_path)
            logger.info('delete: {}'.format(topic_path))
        except:
            pass


class SubscribeClient:
    """Subscriberのラッパークラス."""

    def __init__(self, project):  # noqa: D107
        self.project = project
        self.service = pubsub.SubscriberClient()

    def subscribe_async(self, subscription, callback):
        """通知の購読(非同期)."""
        path = self.service.subscription_path(self.project, subscription)
        return self.service.subscribe(path, callback)

    def subscribe(self, subscription, callback, stop_callback, sleep=1):
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
                if stop_callback():
                    logger.info('stop subscribing')
                    break
        finally:
            future.cancel()

    def delete_subscription(self, subscription):
        """サブスクリプションの削除."""
        try:
            path = self.service.subscription_path(self.project, subscription)
            self.service.delete_subscription(path)
            logger.info('delete: {}'.format(path))
        except:
            pass

    def create_subscription(self, topic_path, subscription):
        """新しいサブスクリプションの作成."""
        path = self.service.subscription_path(self.project, subscription)
        try:
            self.service.create_subscription(path, topic_path)
        except AlreadyExists:
            pass
        return path


@contextmanager
def context(project, topic, subscription):
    """PubSubサブスクリプションのコンテキスト."""
    publisher = PublishClient(project)
    topic_path = publisher.create_topic(topic)
    subscriber = SubscribeClient(project)
    subscribe_path = subscriber.create_subscription(topic_path, subscription)
    logger.info('create: {} ( {} )'.format(subscribe_path, topic_path))
    try:
        yield subscriber
    finally:
        subscriber.delete_subscription(subscription)
        publisher.delete_topic(topic)


def _spinner():
    while True:
        yield '|'
        yield '/'
        yield '-'
        yield '\\'
