#!/usr/bin/env python3

from kafka import KafkaProducer
import sys

def main():
    topic_names = sys.argv[1:]

    producer_config = {
        'bootstrap_servers': 'localhost:9092',
    }

    producer = KafkaProducer(**producer_config)

    for topic_name in topic_names:
        producer.send(topic_name, value=b'')
        producer.flush()

    for line in sys.stdin:
        line = line.strip()
        if line.startswith('comment'):
            producer.send(topic_names[0], value=line.encode('utf-8'))
        elif line.startswith('like'):
            producer.send(topic_names[1], value=line.encode('utf-8'))
        producer.send(topic_names[2], value=line.encode('utf-8'))

    for topic_name in topic_names:
        producer.send(topic_name, value='EOF'.encode('utf-8'))

    producer.close()

if __name__ == '__main__':
    main()

