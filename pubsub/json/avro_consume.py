from confluent_kafka.avro import AvroConsumer
import avro.schema

# Konfigurasi consumer dengan Schema Registry
config = {
    'bootstrap.servers': '34.101.224.54:19092',
    'schema.registry.url': 'http://34.101.224.54:18081',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

# Inisialisasi AvroConsumer
consumer = AvroConsumer(config)
consumer.subscribe(['Task1_Avro_Khairullah'])

# Konsumsi data
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    
    # Data sudah dalam bentuk yang sesuai dengan schema
    stock_data = msg.value()
    print(f"Received stock data: {stock_data}")
