from confluent_kafka import Consumer
import avro.schema
import avro.io
import io

# Definisikan schema Avro (harus sama dengan yang digunakan pada producer)
schema_str = """
{
    "namespace": "Task",
    "type": "record",
    "name": "Stock",
    "fields": [
        {"name": "event_time", "type": "string"},
        {"name": "ticker", "type": "string"},
        {"name": "price", "type": "float"}
    ]
}
"""
schema = avro.schema.parse(schema_str)

# Fungsi untuk melakukan deserialization data dari format Avro
def deserialize_avro(data, schema):
    bytes_reader = io.BytesIO(data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

# Konfigurasi consumer
consumer = Consumer({
    'bootstrap.servers': '34.101.224.54:19092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['stock_avro_topic'])

# Konsumsi data
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    
    # Deserialize data Avro
    avro_data = msg.value()
    stock_data = deserialize_avro(avro_data, schema)
    print(f"Received stock data: {stock_data}")
