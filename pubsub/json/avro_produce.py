from confluent_kafka import Producer
import avro.schema
import avro.io
import io

# Definisikan schema Avro
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

# Fungsi untuk melakukan serialization data ke format Avro
def serialize_avro(data, schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes

# Data yang ingin dikirimkan
stock_data = {
    "event_time": "2023-11-30T14:43:29.700245",
    "ticker": "AMZN",
    "price": 74.95
}

# Serialize data ke Avro
avro_data = serialize_avro(stock_data, schema)

# Konfigurasi producer
producer = Producer({'bootstrap.servers': '34.101.224.54:19092'})

# Mengirim data Avro ke topik
producer.produce('stock_avro_topic', key='some_unique_key', value=avro_data)
producer.flush()
