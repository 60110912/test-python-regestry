#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of AvroSerializer.

from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from typing import List, Optional, Literal
from datetime import datetime
from dataclasses import dataclass, field
from dataclasses_avroschema import AvroModel
import enum

from confluent_kafka.avro import AvroProducer


# @dataclass
# class OpType():
#     symbols: Literal["r", "c", "u", "d" ]
#     class Meta:
#         namespace = "ru.leroymerlin.domain.system.dp"

# @dataclass
# class TableRow():
#     name:
#     class Meta:
#         namespace = "ru.leroymerlin.domain.system.dp"
# @dataclass
# class User(AvroModel):
#     name: str
#     age: int
#     has_pets: bool
#     money: float
@dataclass
class User(AvroModel):
    id: int
    name = 'John Doe'
    signup_ts: Optional[datetime] = None
    friends: List[int] = field(default_factory=list)




def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

def main():
    test_topic = 'test_topic_4'
    schema = User.avro_schema()

    schema_registry_conf = {'url': 'http://localhost:8085'}
    producer_conf = {'bootstrap.servers': 'localhost:9092'}

    avroProducer = AvroProducer(
        {
            'bootstrap.servers': 'localhost:9092',
            "on_delivery": delivery_report,
            "schema.registry.url": "http://localhost:8085",
        },
        default_value_schema=User.avro_schema(),
    )

    value = {
    
    }
    print("Producing user records to topic {}. ^C to exit.".format(test_topic))
    i = 0
    while i < 250:
        # Serve on_delivery callbacks from previous calls to produce()
        value = {
            "id": i,
            "signup_ts": datetime.now(),
            "friends": [
                i - 1,
                i + 1,
                10
            ]
        }
        user = User(**value)
        avroProducer.produce(topic="messages", value=user.to_dict())
        i += 1

    print("\nFlushing records...")
    avroProducer.flush()


if __name__ == '__main__':
    main()