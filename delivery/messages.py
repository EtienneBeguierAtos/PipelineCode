"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1
import time

#TODO(developer)
project_id = "smartlive"
topic_id1 = "my_topic1"
topic_id2 = "my_topic2"
topic_id3 = "my_topic3"

publisher = pubsub_v1.PublisherClient()
topics=[
publisher.topic_path(project_id, topic_id1),
publisher.topic_path(project_id, topic_id2),
publisher.topic_path(project_id, topic_id3)]
publish_futures = []

"""def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback"""
file_path = "./message.json"
def run():
    with open(file_path, 'r') as j:
        data=j.read().splitlines()
        for i in range(len(data)):
            element=data[i]
            print(element)
            #data = str({"name":"Jacques","age":62,"height":1.66})
            # When you publish a message, the client returns a future.
            publish_future1 = publisher.publish(topics[i], element.encode("utf-8"))
            publish_futures.append(publish_future1)
            time.sleep(21)
            #futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
            #publish_future2 = publisher.publish(topic_path2, element.encode("utf-8"))
            #publish_futures.append(publish_future2)
            #futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
            #publish_future3 = publisher.publish(topic_path1, element.encode("utf-8"))
            #publish_futures.append(publish_future3)
        #time.sleep(10)
        #publish_future = publisher.publish(topics[0], data[0].encode("utf-8"))
        #publish_futures.append(publish_future)
        # Wait for all the publish futures to resolve before exiting.
        futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

        print(f"Published messages with error handler.")
if __name__ == '__main__':
    run()