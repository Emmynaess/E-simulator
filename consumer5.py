import json
import random
from kafka import KafkaConsumer, KafkaProducer

consumer_topic = 'ordersprojekt'
consumer_new_topic = 'handledorders'

class HandleOrderSystem:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda x: json.dumps(x).encode('UTF-8')
        )

        self.consumer = KafkaConsumer(
            consumer_topic,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('UTF-8'))
        )

        self.handled_orders_topic = consumer_new_topic
        self.handling_probability = 0.2

    def send_email_simulation(self, order_id):
        email_content = f"Hello! Your order with ID: {order_id} has been shipped. Thank you come again\n"
        with open("emails_log.txt", "a") as email_log:
            email_log.write(email_content)
            print(f"Email sent for order ID: {order_id}")

    def handle_order(self, order):
        if random.random() < self.handling_probability:
            print(f'Handling order {order["order_id"]}')
            self.send_email_simulation(order["order_id"])
            self.producer.send(self.handled_orders_topic, value=order)
            self.producer.flush()
        else:
            print(f'Order {order["order_id"]} was not handled')
    
    def run(self):
        try:
            for message in self.consumer:
                order = message.value
                print(f'Received order: {order["order_id"]}')
                self.handle_order(order)
        except KeyboardInterrupt:
            print('closing consumer 5')
        finally:
            self.producer.close()
            self.consumer.close()

if __name__ == "__main__":
    system = HandleOrderSystem()
    system.run()
