import json
import time
from google.cloud import pubsub_v1

project_id = "trkkn-cloud-playground"
topic_id = "oscar"

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
    time.sleep(1)
