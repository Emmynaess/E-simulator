# Consumer 1
# Utan class, "vanlig" def
"""def consumer_get_orders():
    number_of_orders_today = 0
    todays_date = date.today()

    consumer = KafkaConsumer(
        'orderprojekt',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=3000
    )

    try:
        for message in consumer: 
            if todays_date != date.today(): 
                todays_date = date.today()
                number_of_orders_today = 0

            order_time = message.value['order_time']
            order_time = datetime.strptime(order_time, '%m/%d/%Y-%H:%M:%S')
            order_date = order_time.date()

            if order_date == todays_date: 
                number_of_orders_today += 1
                
            print(f'Number of orders today: {number_of_orders_today}')
    except KeyboardInterrupt:   
        print(f'Closing consumer 1')
    finally:
        consumer.close()

if __name__ == '__main__':
    consumer_get_orders()"""

# Consumer 2
# Utan class, "vanlig def"
"""def read_json_file(path):
    try:
        with open(path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

def write_json_file(path, data):
    with open(path, 'w') as file:
        json.dump(data, file, indent=4)

json_path = 'sales_data.json'

def save_to_json(sale_today, sale_last_hour):
    data = read_json_file(json_path)

    todays_date_str = date.today().strftime('%Y-%m-%d')
    
    if todays_date_str in data:
        data[todays_date_str]['Total Sales Today'] = sale_today
        data[todays_date_str]['Total Sales Last Hour'] = sale_last_hour
    else:
        data[todays_date_str] = {
            'Total Sales Today': sale_today,
            'Total Sales Last Hour': sale_last_hour
        }

    write_json_file(json_path, data)

def consumer_get_sales():
    sale_today = 0
    sale_last_hour = 0
    sale_last_hour_time = datetime.now().hour
    todays_date = date.today()

    consumer = KafkaConsumer(
        'orderprojekt',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
        auto_commit_interval_ms=3000
    )

    try:
        for message in consumer:
            if todays_date != date.today():
                todays_date = date.today()
                date.fromtimestamp
                sale_today = 0

            if sale_last_hour_time != datetime.now().hour:
                sale_last_hour_time = datetime.now().hour
                sale_last_hour = 0

            order_time = message.value['order_time']
            order_time = datetime.strptime(order_time, '%m/%d/%Y-%H:%M:%S')
            order_date = order_time.date()

            if todays_date == order_date:
                for od in message.value['order_details']:
                    sale_today += od['price'] * od['quantity']

            is_same_day = todays_date == order_date
            is_same_hour = sale_last_hour_time == order_time.hour
            if is_same_day and is_same_hour:
                for od in message.value['order_details']:
                    sale_last_hour += od['price'] * od['quantity']

            print(f'Sales:\n Total sales today: {sale_today}\n Total sales last hour: {sale_last_hour}')
            save_to_json(sale_today, sale_last_hour)
            
    except KeyboardInterrupt:
        print('Closing consumer 2.')
    finally:
        consumer.close()
       
if __name__ == '__main__':
    consumer_get_sales()"""

# Consumer 3
# Utan class, "vanlig" def
"""orders_count = 0
total_sales = 0
product_sales = {}

def save_report_to_file(todays_date, orders_count, total_sales, product_sales):
    report_filename = f"report_{todays_date.strftime('%Y-%m-%d')}.txt"

    with open(report_filename, 'w', encoding='UTF-8') as report_file:
        report_file.write(f"Daily Report for {todays_date.strftime('%Y-%m-%d')}\n")
        report_file.write(f"Total Orders: {orders_count}\n")
        report_file.write(f"Total Sales: {total_sales}\n")
        report_file.write("Sales per Product:\n")
        for product, quantity in product_sales.items():
            report_file.write(f"{product}: {quantity}\n")
    print(f"Report saved to {report_filename}")

def consumer_process_orders():
    global orders_count, total_sales, product_sales
    consumer = KafkaConsumer(
        "orders",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
        auto_commit_interval_ms=3000
    )
    previous_date = datetime.now().date()

    try:
        for message in consumer:
            order_time_str = message.value["order_time"]
            order_time = datetime.strptime(order_time_str, "%m/%d/%Y-%H:%M:%S")
            current_order_date = order_time.date()

            if current_order_date != previous_date:
                save_report_to_file(previous_date, orders_count, total_sales, product_sales)
                orders_count = 0
                total_sales = 0
                product_sales = {}
                previous_date = current_order_date

            orders_count += 1
            for item in message.value["order_details"]:
                total_sales += item["price"] * item["quantity"]
                product_name = item["product_name"]
                product_sales[product_name] = product_sales.get(product_name, 0) + item["quantity"]

    except KeyboardInterrupt:
        print('Closing consumer')

    finally:
        consumer.close()
        save_report_to_file(previous_date, orders_count, total_sales, product_sales)

if __name__ == "__main__":
    consumer_process_orders()"""

# Consumer 4, def
"""PRODUCTS_DB_PATH = "C:\\Users\\EmmyN\\.Programmering\\Projektarbete\\products.db"
ORDER_DESCENDING_TXT = 'descending_count.txt'
ORDER_DESCENDING_JSON = 'descending_count.json'
new_stock = 0

def consumer_order_descending(handled_orders:set):
    global new_stock
    consumer = KafkaConsumer(
        'orderprojekt',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=3000
    )

    try:
        db = sqlite3.connect(PRODUCTS_DB_PATH)
        cursor = db.cursor()
        for message in consumer:
            message = message.value
            print(f"Received order with ID: {message['order_id']}")
            if message["order_id"] in handled_orders:
                print(f"Order ID {message['order_id']} already handled.")
                continue 
            
            order_details_to_save = []

            with open(ORDER_DESCENDING_TXT, 'a', encoding='UTF-8') as file:
                for item in message["order_details"]:
                    product = cursor.execute("SELECT * FROM products WHERE productid = ?", (item["product_id"],)).fetchone()
                    if product:
                        in_stock = product[5]
                        new_stock = in_stock - item['quantity']  
                        print(f"Order ID {message['order_id']}\nProduct ID {item['product_id']} - Stock before: {in_stock}, Stock after: {new_stock}")
                        file.write(f"Order ID {message['order_id']}\nProduct ID {item['product_id']} - Stock before: {in_stock}, Stock after: {new_stock}\n")

                        order_details_to_save.append({
                            "order_id": message["order_id"],
                            "product_id": item["product_id"],
                            "stock_before": in_stock,
                            "stock_after": new_stock
                        })

                    cursor.execute("UPDATE products SET quantity = ? WHERE productid = ?", (new_stock, item["product_id"]))
                    db.commit()

            
            with open(ORDER_DESCENDING_JSON, 'a', encoding='UTF-8') as json_file:
                json.dump(order_details_to_save, json_file, indent=4)
                json_file.write("\n")

            handled_orders.add(message["order_id"])

    except KeyboardInterrupt:
        print('\nClosing consumer 3')
    except Exception as e:
        print(f'An error occured {e}')
    
    finally:
        consumer.close()
        db.close()

if __name__ == "__main__":
    handled_orders = set()
    consumer_order_descending(handled_orders)"""


# Consumer 4
"""import json
import sqlite3
import os
from kafka import KafkaConsumer
from datetime import date, datetime, timedelta

PRODUCTS_DB_PATH = "C:\\Users\\EmmyN\\.Programmering\\Projektarbete\\products.db"
ORDER_DESCENDING_TXT = 'descending_count.txt'
new_stock = 0

def consumer_order_descending(handled_orders:set):
    global new_stock
    consumer = KafkaConsumer(
        'orderprojekt',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=3000
    )

    try:
        db = sqlite3.connect(PRODUCTS_DB_PATH)
        cursor = db.cursor()
        for message in consumer:
            message = message.value
            print(f"Received order with ID: {message['order_id']}")
            if message["order_id"] in handled_orders:
                print(f"Order ID {message['order_id']} already handled.")
                continue 

            with open(ORDER_DESCENDING_TXT, 'a', encoding='UTF-8') as file:
                for item in message["order_details"]:
                    product = cursor.execute("SELECT * FROM products WHERE productid = ?", (item["product_id"],)).fetchone()
                    if product:
                        in_stock = product[5]
                        new_stock = in_stock - item['quantity']  
                        print(f"Order ID {message['order_id']}\nProduct ID {item['product_id']} - Stock before: {in_stock}, Stock after: {new_stock}")
                        file.write((f"Order ID {message['order_id']}\nProduct ID {item['product_id']} - Stock before: {in_stock}, Stock after: {new_stock}\n"))
                    cursor.execute("UPDATE products SET quantity = ? WHERE productid = ?", (new_stock, item["product_id"]))
                    db.commit()
            handled_orders.add(message["order_id"])

    except KeyboardInterrupt:
        print('Closing consumer 4')
    except Exception as e:
        print(f'An error occured {e}')
    
    finally:
        consumer.close()
        db.close()

if __name__ == "__main__":
    handled_orders = set()
    consumer_order_descending(handled_orders)"""


# consumer 4
"""with open(ORDER_DESCENDING_TXT, 'a', encoding='UTF-8') as file:
                print(f"Writing to file: {message['order_id']}")
                file.write(str(message['order_id'])+ '\n')"""


# consumer 3, try, and try agaaaaain
"""class OrderReportConsumer:
    def __init__(self):
        self.orders_count = 0
        self.total_sales = 0
        self.product_sales = {}
        self.consumer = KafkaConsumer(
            "orderprojekt",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,
            auto_commit_interval_ms=3000
        )
        self.previous_date = datetime.now().date()

    def save_report_to_file(self):
        report_filename = f"report_{self.previous_date.strftime('%Y-%m-%d')}.txt"
        with open(report_filename, 'w', encoding='UTF-8') as report_file:
            report_file.write(f"Daily Report for {self.previous_date.strftime('%Y-%m-%d')}\n")
            report_file.write(f"Total Orders: {self.orders_count}\n")
            report_file.write(f"Total Sales: {self.total_sales}\n")
            report_file.write("Sales per Product:\n")
            for product, quantity in self.product_sales.items():
                report_file.write(f"{product}: {quantity}\n")
        print(f"Report saved to {report_filename}")

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
            print('Closing consumer')

        finally:
            self.consumer.close()
            self.save_report_to_file()

if __name__ == "__main__":
    order_report_consumer = OrderReportConsumer()
    order_report_consumer.process_orders()"""


# Consumer 5 och 6
# ----- Test, test, testa igen! Dela upp koden!
"""input_topic = 'orderprojekt'
output_topic = 'handledorders'

producer = KafkaProducer(
    output_topic,
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def send_email_simulation(order_id):
    email_content = f"Order with ID: {order_id} has been shipped.\n"
    with open("emails_log.txt", "a") as email_log:
        email_log.write(email_content)
    print(f"Email sent for order ID: {order_id}")

def handle_order(order):
    handling_probability = 0.2 
    if random.random() < handling_probability:
        print(f"Handling order {order['order_id']}")
        send_email_simulation(order['order_id'])
        producer.send(output_topic, value=order)
        producer.flush()
    else:
        print(f"Order {order['order_id']} was not handled.")

for message in consumer:
    print(f"Received order: {message.value}")
    handle_order(message.value)


# Deque för att hålla reda på order tidsstämplar för varje tidsintervall
orders_timestamps = {interval: deque() for interval in intervaller_tid}

def update_statistics(order_time):
    current_time = datetime.now()
    for intervall_name, intervall_time in intervaller_tid.items():
        while orders_timestamps[intervall_name] and orders_timestamps[intervall_name][0] < current_time - intervall_time:
            orders_timestamps[intervall_name].popleft()
        print(f"Orders in the last {intervall_name}: {len(orders_timestamps[intervall_name])}")

for message in consumer:
    order = message.value
    order_time = datetime.strptime(order['order_time'], "%m/%d/%Y-%H:%M:%S")

    for interval in intervaller_tid:
        orders_timestamps[interval].append(order_time)
    update_statistics(order_time)"""


#bootstrap_servers = 'localhost:9092'
#orders_topic = 'orderprojekt'
#handled_orders_topic = 'handledorders'

# Sannolikhet för att en order ska hanteras
"""handling_probability = 0.2  # 20% chans att ordern hanteras

producer = KafkaProducer(
    'handled_orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_commit_interval_ms=3000
)

consumer = KafkaConsumer(
    'ordersprojekt',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_commit_interval_ms=3000
)

def handle_order(order):
    # kod, bygg vidare här!
    print(f"Order with ID {order['order_id']} has been handled.")
    # Skicka ett meddelande om att ordern är skickad..här?
    send_order_handled_confirmation(order)

def send_order_handled_confirmation(order):
    # kod, bygg vidare här!
    email_confirmation = f"Order with ID {order['order_id']} was skickad on {datetime.now().isoformat()}"
    with open('handled_orders_email_confirmation.txt', 'a') as file:
        file.write(email_confirmation + '\n')
    producer.send(handle_order, value=order)
    producer.flush()

for message in consumer:
    order = message.value
    print(f"Received order: {order}")
    # Slumpen?? Testa med random här
    if random.random() < handling_probability:
        handle_order(order)
    else:
        print(f"Order with ID {order['order_id']} was not selected for handling.")

    # ------------------------------------------
    # Testar koden igen här med lite modifieringar
handling_probability = 0.2  # 20% chans att ordern hanteras
    # Konfiguration för Kafka
bootstrap_servers = 'localhost:9092'
output_topic = 'handled_orders'

# Skicka hanterade ordrar till en ny topic handledorders
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def send_email_simulation(order_id):
    email_content = f"Order with ID: {order_id} has been skickad.\n"
    with open("emails_log.txt", "a") as email_log:
        email_log.write(email_content)

def handle_order(order):
    if random.random() < 0.5:
        print(f"Handling order {order['order_id']}")
        send_email_simulation(order['order_id'])
        producer.send(output_topic, value=order)
        producer.flush()
    else:
        print(f"Order {order['order_id']} was not handled.")

print("Starting order consumer...")
for message in consumer:
    print(f"Received order: {message.value}")
    handle_order(message.value)"""