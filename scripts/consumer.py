from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

def consume_messages():
    consumer_conf = {
        'bootstrap.servers': 'kafka-1:9092,kafka-2:9093',
        'group.id': 'twitter-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(**consumer_conf)
    consumer.subscribe(['twitter-stream'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition()} at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            
            else:
                print(f"Consumed message from topic {msg.topic()}: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        print("Aborted by user")

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()