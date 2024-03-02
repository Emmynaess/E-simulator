from kafka import KafkaConsumer
import json
from datetime import datetime, date, time
import os

consumer_topic = 'ordersprojekt' 

class SalesConsumer:
    def __init__(self, json_path='sales_data.json'):
        self.json_path = json_path
        self.sale_today = 0
        self.sale_last_hour = 0
        self.sale_last_hour_time = datetime.now().hour
        self.todays_date = date.today()
        self.consumer = KafkaConsumer(
            consumer_topic,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,
            auto_commit_interval_ms=3000
        )

    def read_json_file(self):
        try:
            with open(self.json_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return {}

    def write_json_file(self, data):
        with open(self.json_path, 'w') as file:
            json.dump(data, file, indent=4)

    def save_to_json(self):
        data = self.read_json_file()

        todays_date_str = date.today().strftime('%Y-%m-%d')
        
        if todays_date_str in data:
            data[todays_date_str]['Total Sales Today'] = self.sale_today
            data[todays_date_str]['Total Sales Last Hour'] = self.sale_last_hour
        else:
            data[todays_date_str] = {
                'Total Sales Today': self.sale_today,
                'Total Sales Last Hour': self.sale_last_hour
            }

        self.write_json_file(data)
    
    def print_sales_clear_console(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        print(f'Sales:\n Total sales today: {self.sale_today}\n Total sales last hour: {self.sale_last_hour}')

    def get_sales(self):
        try:
            for message in self.consumer:
                if self.todays_date != date.today():
                    self.todays_date = date.today()
                    self.sale_today = 0

                if self.sale_last_hour_time != datetime.now().hour:
                    self.sale_last_hour_time = datetime.now().hour
                    self.sale_last_hour = 0

                order_time = message.value['order_time']
                order_time = datetime.strptime(order_time, '%m/%d/%Y-%H:%M:%S')
                order_date = order_time.date()

                if self.todays_date == order_date:
                    for od in message.value['order_details']:
                        self.sale_today += od['price'] * od['quantity']

                is_same_day = self.todays_date == order_date
                is_same_hour = self.sale_last_hour_time == order_time.hour
                if is_same_day and is_same_hour:
                    for od in message.value['order_details']:
                        self.sale_last_hour += od['price'] * od['quantity']

                self.print_sales_clear_console()
                self.save_to_json()
                    
        except KeyboardInterrupt:
            print('Closing consumer 2.')
        finally:
            self.consumer.close()

if __name__ == "__main__":
    sales_consumer = SalesConsumer()
    sales_consumer.get_sales()
