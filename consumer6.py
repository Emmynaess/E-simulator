from kafka import KafkaConsumer
from collections import deque
from datetime import datetime, timedelta
import json
import os

consumer_new_topic = 'handledorders'

class OrderTimeIntervall:
    def __init__(self, handled_orders_topic=consumer_new_topic):
        self.consumer = KafkaConsumer(
            handled_orders_topic,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('UTF-8'))
        )
        self.order_timestamps = deque()
        self.time_intervall = {
            '5 minutes': timedelta(minutes=5),
            '30 minutes': timedelta(minutes=30),
            '60 minutes': timedelta(hours=1),
            '120 minutes': timedelta(hours=2)
        }

    def clear_console(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        
        #if os.name == 'nt':
        #    os.system('cls')


    def update_statistics(self):
        self.clear_console()
        now = datetime.now()
        while self.order_timestamps and self.order_timestamps[0] < now - self.time_intervall['120 minutes']:
            self.order_timestamps.popleft()

        intervall = {}
        for name, interval in self.time_intervall.items():
            count = sum(1 for timestamp in self.order_timestamps if timestamp >= now - interval)
            intervall[name] = count

        total_orders_in_last_2_hours = sum(1 for timestamp in self.order_timestamps if timestamp >= now - self.time_intervall['120 minutes'])
        number_of_5_minute_in_2_hours = 24
        average = total_orders_in_last_2_hours / number_of_5_minute_in_2_hours
        intervall['Average every 5 minutes in the last 2 hours'] = average

        print(f"Total orders:\n"
              f"- Total orders last 5 min: {intervall['5 minutes']}\n"
              f"- Total orders last 30 min: {intervall['30 minutes']}\n"
              f"- Total orders last 60 min: {intervall['60 minutes']}\n"
              f"- Total orders last 120 min: {intervall['120 minutes']}\n"
              f"- Average every 5 minutes in the last 2 hours: {average:.2f}")

    def run(self):
        try:
            for message in self.consumer:
                order_info = message.value
                order_time = order_info["order_time"]
                order_timestamp = datetime.strptime(order_time, "%m/%d/%Y-%H:%M:%S")
                self.order_timestamps.append(order_timestamp)
                self.update_statistics()
        except KeyboardInterrupt:
            print('closing consumer 6')
        finally:
            self.consumer.close()

if __name__ == "__main__":
    stats = OrderTimeIntervall()
    stats.run()
    