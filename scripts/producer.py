import random
import json
import time
from confluent_kafka import Producer

# Configuración del producer para Kafka
producer_conf = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9093',
}

producer = Producer(**producer_conf)

# Lista de usuarios ficticios
users = ['user1', 'user2', 'user3', 'user4']

# Lista de palabras clave o temas
keywords = ['bitcoin', 'ethereum', 'AI', 'machine learning', 'cloud computing']

def generate_tweet():
    tweet = {
        'text': f"{random.choice(keywords)} is great! {random.randint(0, 100)}",
        'user': random.choice(users),
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    }
    return tweet

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar el mensaje: {err}")
    else:
        print(f"Mensaje enviado exitosamente: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def produce_fake_tweets():
    while True:
        tweet = generate_tweet()
        producer.produce('twitter-stream', key=tweet['user'], value=json.dumps(tweet), callback=delivery_report)
        producer.poll(0)
        time.sleep(1)  # Pausa de 1 segundo entre tweets para simular un flujo en tiempo real

if __name__ == "__main__":
    produce_fake_tweets()
