from kafka import KafkaConsumer
import json
from datetime import datetime, date
import sys

consumer_topic = 'ordersprojekt'

class OrderConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            consumer_topic,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_commit_interval_ms=3000
        )
        self.number_of_orders_today = 0
        self.todays_date = date.today()

    def print_sales(self):
        sys.stdout.write(f'\rNumber of orders today: {self.number_of_orders_today}               ')
        sys.stdout.flush()

    def get_orders(self):
        try:
            for message in self.consumer:
                if self.todays_date != date.today():
                    self.todays_date = date.today()
                    self.number_of_orders_today = 0

                order_time = message.value['order_time']
                order_time = datetime.strptime(order_time, '%m/%d/%Y-%H:%M:%S')
                order_date = order_time.date()

                if order_date == self.todays_date:
                    self.number_of_orders_today += 1

                self.print_sales()
        except KeyboardInterrupt:
            print('\nClosing consumer 1')
        finally:
            self.consumer.close()

if __name__ == "__main__":
    order_consumer = OrderConsumer()
    order_consumer.get_orders()

    