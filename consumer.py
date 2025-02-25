from confluent_kafka import Consumer, KafkaError

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'notification-group',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['email-topic', 'sms-topic'])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f'Received message from topic {msg.topic()}: {msg.value().decode("utf-8")}')

except KeyboardInterrupt:
    pass

finally:
    c.close()

# from kafka import KafkaConsumer
# import pandas as pd

# consumer = KafkaConsumer('my_topic',
#                          bootstrap_servers=['kafka_broker:9092'],
#                          auto_offset_reset='earliest',
#                          enable_auto_commit=False,
#                          group_id='my_group_id',
#                          value_deserializer=lambda x: x.decode('utf-8')
#                         )

# for message in consumer:
#     data = message.value.decode("utf-8")
#     df = pd.read_json(data)
#     print("Received data: ", df.head())