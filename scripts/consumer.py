from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from textblob import TextBlob
from pyspark.sql.functions import udf
from cassandra.cluster import Cluster

# Configuracion del Consumer
consumer_conf = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9093',  # Brokers de Kafka
    'group.id': 'twitter-group',  # Grupo de consumidores
    'auto.offset.reset': 'earliest'  # Empezar desde el principio si no hay offset
}

# Conectar al clúster de Cassandra

cluster = Cluster(['cassandra'])
session = cluster.connect()

# Interactuar con Cassandra

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS my_keyspace
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}
""")

session.set_keyspace('my_keyspace')

session.execute("""
    CREATE TABLE IF NOT EXISTS tweets (
        id UUID PRIMARY KEY,
        user text,
        text text,
        timestamp text,
        sentiment float            
    )
""")

# Crear una instancia del Consumer
consumer = Consumer(**consumer_conf)
consumer.subscribe(['twitter-stream'])

# Crear una sesion de Spark
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis") \
    .getOrCreate()

# Definir la estructura del tweet
tweet_schema = StructType([
    StructField("text", StringType(), True),
    StructField("user", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Funcion UDF para analisis de sentimientos
def sentiment_analysis(text):
    analysis = TextBlob(text)
    return float(analysis.sentiment.polarity)

sentiment_analysis_udf = udf(sentiment_analysis)

def process_message(message):
    # Procesar el mensaje recibido (en este caso, un tweet simulado)
    tweet_data = json.loads(message)
    print("Tweet de {}: {} (enviado en {})".format(tweet_data['user'], tweet_data['text'], tweet_data['timestamp']))
    
    # Convertir a un DataFrame de Spark
    tweet_df = spark.createDataFrame([tweet_data], schema=tweet_schema)
    
    # Aplicar el analisis de sentimientos
    tweets_with_sentiment = tweet_df.withColumn("sentiment", sentiment_analysis_udf(col("text")))
    
    # Mostrar los datos con analisis de sentimientos
    tweets_with_sentiment.show()

    # Insertar los datos en Cassandra
    sentiment_value = float(tweets_with_sentiment.select("sentiment").collect()[0]["sentiment"])
    insert_statement = session.prepare("INSERT INTO tweets (id, user, text, timestamp, sentiment) VALUES (uuid(), ?, ?, ?, ?)")
    session.execute(insert_statement, (tweet_data['user'], tweet_data['text'], tweet_data['timestamp'], sentiment_value))

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Esperar mensajes

        if msg is None:
            continue  # No hay mensajes en la cola, continuar esperando

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Se alcanzo el final de la particion
                print("End of partition reached {} at offset {}".format(msg.partition(), msg.offset))
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
    spark.stop()  # Cerrar la sesion de Spark
    session.shutdown()