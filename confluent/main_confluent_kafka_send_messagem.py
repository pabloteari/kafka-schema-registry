
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro

producer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'schema.registry.url': 'http://localhost:8081'
}

producer = AvroProducer(producer_conf)
topic = 'meu_topico'

value_schema = avro.loads('{"type": "record", "name": "myrecord", "fields": [{"name":"f1","type":"string"}]}')

record_value = {"f1": "Mensagem de exemplo"}

producer.produce(topic=topic, value=record_value, value_schema=value_schema)

producer.flush()

print("Mensagem enviada com sucesso para o tópico 'meu_topico'")
