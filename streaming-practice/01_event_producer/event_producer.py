import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers='kafka:9092', 
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_purchase_event():
    """Fungsi untuk menghasilkan event pembelian"""
    event = {
        'purchase_id': random.randint(1000, 9999),
        'amount': round(random.uniform(20.5, 200.0), 2),
        'timestamp': (datetime.now() - timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')  # Timestamps terlambat 5 menit
    }
    return event

def produce_event():
    """Memproduksi event setiap 5 detik dengan keterlambatan 1 menit"""
    while True:
        event = generate_purchase_event()
        producer.send('purchases_topic', value=event)
        print(f"Produced event: {event}")
        time.sleep(30)  # Tunggu 30 detik sebelum menghasilkan event berikutnya

if __name__ == '__main__':
    produce_event()
