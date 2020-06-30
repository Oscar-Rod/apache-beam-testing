import json
import os
import time

from google.cloud import pubsub_v1

project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
topic_id = os.environ["topic"].split("/")[-1]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def generate_message(user, products):
    return json.dumps({"user": user, "products": products,}).encode("utf-8")


messages = [
    generate_message("user_1", [{"id": "prod_1", "quantity": 1,}]),
    generate_message("user_1", [{"id": "prod_1", "quantity": 1,}]),
    generate_message("user_1", [{"id": "prod_1", "quantity": 1,}]),
    generate_message("user_2", [{"id": "prod_1", "quantity": 2,}]),
    generate_message("user_2", [{"id": "prod_1", "quantity": 2,}]),
    generate_message("user_2", [{"id": "prod_1", "quantity": 2,}]),
    generate_message("user_3", [{"id": "prod_1", "quantity": 3,}]),
    generate_message("user_3", [{"id": "prod_1", "quantity": 3,}]),
    generate_message("user_3", [{"id": "prod_1", "quantity": 3,}]),
]

for message in messages:
    publisher.publish(topic_path, data=message)
    # time.sleep(1)
