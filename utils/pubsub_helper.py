"""
utils/pubsub_helper.py

Centralizes Pub/Sub client creation.
Avoids repeating subscriber setup across consumer and archiver.
"""

from google.cloud import pubsub_v1
from config.settings import settings


def get_subscriber(subscription_name: str):
    """
    Creates a SubscriberClient and returns both the client
    and the fully qualified subscription path.
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        settings.gcp_project_id,
        subscription_name
    )
    return subscriber, subscription_path


def get_publisher():
    """
    Creates a PublisherClient and returns both the client
    and the fully qualified topic path.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        settings.gcp_project_id,
        settings.pubsub_topic
    )
    return publisher, topic_path