from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import sys

# Configuración del Consumer
consumer_conf = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9093',  # Brokers de Kafka
    'group.id': 'twitter-group',  # Grupo de consumidores
    'auto.offset.reset': 'earliest'  # Empezar desde el principio si no hay offset
}

# Crear una instancia del Consumer
consumer = Consumer(**consumer_conf)

# Suscribirse al topic de Kafka
consumer.subscribe(['twitter-stream'])

def process_message(message):
    # Procesar el mensaje recibido (en este caso, un tweet simulado)
    tweet_data = json.loads(message)
    print(f"Tweet de {tweet_data['user']}: {tweet_data['text']} (enviado en {tweet_data['timestamp']})")
    # Aquí puedes realizar un análisis de sentimientos o cualquier otra operación

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Esperar mensajes

        if msg is None:
            continue  # No hay mensajes en la cola, continuar esperando

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Se alcanzó el final de la partición
                print(f"End of partition reached {msg.partition()} at offset {msg.offset()}")
            else:
                # Otro error
                raise KafkaException(msg.error())

        else:
            # Mensaje recibido correctamente
            process_message(msg.value().decode('utf-8'))

except KeyboardInterrupt:
    # Permitir salida segura con Ctrl+C
    print("Interrumpido por el usuario")

finally:
    # Asegurarse de cerrar el consumer correctamente
    consumer.close()