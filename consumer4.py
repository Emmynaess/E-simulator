import json
import sqlite3
from kafka import KafkaConsumer

consumer_topic = 'ordersprojekt'
DB_PATH_WAY = 'products.db'

class OrderDescendingConsumer:
    def __init__(self, db_path, json_path):
        self.db_path = db_path
        self.json_path = json_path
        self.handled_orders = set()
        self.new_stock = 0
        self.consumer = KafkaConsumer(
            consumer_topic,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_commit_interval_ms=3000
        )
        self.db = sqlite3.connect(self.db_path)
        self.cursor = self.db.cursor()

    def process_order(self, message):
        if message["order_id"] in self.handled_orders:
            return

        order_details_to_save = []

        for item in message["order_details"]:
            product = self.cursor.execute("SELECT * FROM products WHERE productid = ?", (item["product_id"],)).fetchone()
            if product:
                in_stock = product[5]
                self.new_stock = in_stock - item['quantity']  
                print(f"Order ID {message['order_id']}\nProduct ID {item['product_id']} - Stock before: {in_stock}, Stock after: {self.new_stock}")

                order_details_to_save.append({
                    "order_id": message["order_id"],
                    "product_id": item["product_id"],
                    "stock_before": in_stock,
                    "stock_after": self.new_stock
                })

            self.cursor.execute("UPDATE products SET quantity = ? WHERE productid = ?", (self.new_stock, item["product_id"]))
            self.db.commit()

        with open(self.json_path, 'a', encoding='UTF-8') as json_file:
            json.dump(order_details_to_save, json_file, indent=4)
            json_file.write("\n")

        self.handled_orders.add(message["order_id"])

    def consume_orders(self):
        try:
            for message in self.consumer:
                self.process_order(message.value)

        except KeyboardInterrupt:
            print('\nClosing consumer 4')
        except Exception as e:
            print(f'An error occurred: {e}')
        finally:
            self.consumer.close()
            self.db.close()

if __name__ == "__main__":
    consumer = OrderDescendingConsumer(
        DB_PATH_WAY,
        'descending_count.json'
    )
    consumer.consume_orders()