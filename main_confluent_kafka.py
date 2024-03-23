from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer, AvroConsumer

schema_registry_url = "http://localhost:8081"

producer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'schema.registry.url': schema_registry_url
}

consumer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'my_consumer_group',
    'schema.registry.url': schema_registry_url
}

producer = AvroProducer(producer_conf)

consumer = AvroConsumer(consumer_conf)
consumer.subscribe(['meu_topico'])

topic = 'meu_topico'
value_schema = avro.loads('{"type": "record", "name": "myrecord", "fields": [{"name":"f1","type":"string"}]}')
record_value = {"f1": "value1"}
producer.produce(topic=topic, value=record_value, value_schema=value_schema)
producer.flush()

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    print('Received message: {}'.format(msg.value()))
