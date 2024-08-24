from confluent_kafka import Producer
import json
import time

#Configuración del producer, intruyendo la conexión entre el clúster kafka.
producer_conf= {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9093',
}

producer = Producer(**producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Error message: {err}")
    else:
        print(f"Succesfull message: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def produce_messages():
    while True:
        data = {'event':'test_message','timestamp': time.time()}
        producer.produce('twitter-stream', key='key',value=json.dumps(data),callback=delivery_report)
        producer.poll(0)
        time.sleep(1)

if __name__ == "__main__":
    produce_messages()
    producer.flush()