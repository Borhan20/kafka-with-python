from confluent_kafka import Producer
import time

p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_notification(topic, payload):
    p.produce(topic, payload.encode('utf-8'), callback=delivery_report)
    p.poll(0)

    print(f'Message sent to {topic}: {payload}')

while True: 

    time.sleep(10)
    # Usage example
    email_payload = '{"to": "receiver@example.com", "from": "sender@example.com", "subject": "Sample Email", "body": "This is a sample email notification"}'
    send_notification('email-topic', email_payload)

    sms_payload = '{"phoneNumber": "1234567890", "message": "This is a sample SMS notification"}'
    send_notification('sms-topic', sms_payload)

    time.sleep(10)
    p.flush()





# from kafka import KafkaProducer

# producer = KafkaProducer(
#     bootstrap_servers=["kafka_broker:9092"]
# )

# producer.send("topic_name", value="Hello, World!".encode("utf-8"))