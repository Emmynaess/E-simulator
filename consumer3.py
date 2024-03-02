import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import time
import threading

consumer_topic = 'ordersprojekt'

class OrderReportConsumer:
    def __init__(self):
        self.orders_count = 0
        self.total_sales = 0
        self.product_sales = {}
        self.consumer = KafkaConsumer(
            consumer_topic,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,
            auto_commit_interval_ms=3000
        )
        self.previous_date = datetime.now().date()
        countdown_thread = threading.Thread(target=self.countdown_to_midnight)
        countdown_thread.daemon = True
        countdown_thread.start()

    def save_report_to_file(self):
        report_filename = f"report_{self.previous_date.strftime('%Y-%m-%d')}.txt"
        with open(report_filename, 'w', encoding='UTF-8') as report_file:
            report_file.write(f"Daily Report for {self.previous_date.strftime('%Y-%m-%d')}\n")
            report_file.write(f"Total Orders: {self.orders_count}\n")
            report_file.write(f"Total Sales: {self.total_sales}\n")
            report_file.write("Sales per Product:\n")
            for product, quantity in self.product_sales.items():
                report_file.write(f"{product}: {quantity}\n")
        #print(f"\nReport saved to {report_filename}")

    def countdown_to_midnight(self):
        while True:
            now = datetime.now()
            midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            countdown = (midnight - now).total_seconds()
            if countdown < 1:
                print("\nCreating daily report")
                time.sleep(1)
            else:
                hours, remainder = divmod(int(countdown), 3600)
                minutes, seconds = divmod(remainder, 60)
                print(f"\rTime until next report: {hours:02d}:{minutes:02d}:{seconds:02d}", end="")
                time.sleep(1)

    def process_orders(self):
        try:
            for message in self.consumer:
                order_time_str = message.value["order_time"]
                order_time = datetime.strptime(order_time_str, "%m/%d/%Y-%H:%M:%S")
                current_order_date = order_time.date()

                if current_order_date != self.previous_date:
                    self.save_report_to_file()
                    self.orders_count = 0
                    self.total_sales = 0
                    self.product_sales = {}
                    self.previous_date = current_order_date

                self.orders_count += 1
                for item in message.value["order_details"]:
                    self.total_sales += item["price"] * item["quantity"]
                    product_name = item["product_name"]
                    self.product_sales[product_name] = self.product_sales.get(product_name, 0) + item["quantity"]

        except KeyboardInterrupt:
            print('Closing consumer 3')

        finally:
            self.consumer.close()
            self.save_report_to_file()

if __name__ == "__main__":
    order_report_consumer = OrderReportConsumer()
    order_report_consumer.process_orders()
