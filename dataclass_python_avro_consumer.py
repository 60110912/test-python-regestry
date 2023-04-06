from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from typing import List, Optional, Literal
from datetime import datetime
from dataclasses import dataclass, field
from dataclasses_avroschema import AvroModel

@dataclass
class User(AvroModel):
    id: int
    name = 'John Doe'
    signup_ts: Optional[datetime] = None
    friends: List[int] = field(default_factory=list)

def main():
    test_topic = 'messages'
    c = AvroConsumer(
        {
            'bootstrap.servers': 'localhost:9092',
            "group.id": "avro-consumer_3",
            "schema.registry.url": "http://localhost:8085",
        }
    )
    print('Init AVRO consumer')
    c.subscribe([test_topic])
    print('Init AVRO consumer')
    i=0
    while i<1000:
        i += 1
        print(f'make {i} records')
        try:
            print(f'Получаем данные из POLL')
            msg = c.poll()

        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            break

        if msg is None:
            print(f'Данные пустые')
            continue

        if msg.error():
            print("AvroConsumer error: {}".format(msg.error()))
            continue

        print(User(**msg.value()))

    c.close()


if __name__ == '__main__':
    main()