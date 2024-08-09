from confluent_kafka.avro import AvroProducer
import avro.schema

# Load schema Avro dari file stock.avsc
schema_path = "path/to/your/stock.avsc"
schema = avro.schema.parse(open(schema_path, "rb").read())

# Konfigurasi producer dengan Schema Registry
config = {
    'bootstrap.servers': '34.101.224.54:19092',
    'schema.registry.url': 'http://34.101.224.54:18081'
}

# Data yang ingin dikirimkan
stock_data = {
    "event_time": "2023-11-30T14:43:29.700245",
    "ticker": "AMZN",
    "price": 74.95
}

# Inisialisasi AvroProducer
producer = AvroProducer(config, default_value_schema=schema)

# Mengirim data Avro ke topik
producer.produce(topic='Task1_Avro_Khairullah', value=stock_data)
producer.flush()
