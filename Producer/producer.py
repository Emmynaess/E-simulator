from kafka import KafkaProducer
from producer_sql import producer_db
from sqlite3 import Cursor
import random
import json
import datetime
import time

CUSTOMER_ID = [id for id in range(10000,13000)]
MU = 2
SIGMA = 1

def random_products(
        number_of_products_to_random:int, 
        number_of_products_in_db:int, 
        cursor:Cursor
        ) -> list[dict]:
    
    list_of_random_products = []

    for _ in range(number_of_products_to_random):
        random_product_id = random.randint(1,number_of_products_in_db)
        item = cursor.execute(f"SELECT * FROM products WHERE productid={random_product_id}").fetchone()

        quantity = random.randint(1,10)

        if quantity > item[5]: quantity = 0

        random_product = {"product_id": item[0],
                          "product_name": item[1],
                          "product_type": item[2],
                          "pricetype": item[3],
                          "price":item[4],
                          "quantity":quantity}
        
        list_of_random_products.append(random_product)

    return list_of_random_products

def random_order(order_id:int, number_of_products:int, cursor:Cursor) -> dict:
    customer_id = random.choice(CUSTOMER_ID)
    products = random_products(random.randint(1,2), number_of_products, cursor)
    order_time = datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S")

    order_data = dict(order_id=order_id,
                     customer_id=customer_id,
                     order_details=products,
                     order_time=order_time) 
    return order_data


if __name__ == "__main__":

    order_id = 100000

    cursor = producer_db()

    products = cursor.execute("SELECT * FROM products").fetchall()
    number_of_products_in_database = len(products)

    producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode(encoding='utf-8')
    )
    try:
        while True:
            time.sleep(2)
            random_gauss = int(random.gauss(mu=MU, sigma=SIGMA))
            for _ in range(random_gauss):
                order_id += 1
                order_data = random_order(order_id, number_of_products_in_database, cursor)
                producer.send("ordersprojekt", order_data)

            producer.flush()

    except KeyboardInterrupt:
        print('Closing producer')
    
    finally:
        producer.flush()
        producer.close()
        cursor.close()
        
