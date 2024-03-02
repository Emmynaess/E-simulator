import sqlite3

PRODUCTS_DB_PATH = "products.db"
PRODUCTS_FILE = "products.txt"

def producer_db() -> sqlite3.Cursor:

    db = sqlite3.connect(PRODUCTS_DB_PATH)

    cursor = db.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS products 
                (productid INTEGER PRIMARY KEY AUTOINCREMENT,
                productname TEXT,
                type TEXT, 
                pricetype TEXT, 
                price INTEGER, 
                quantity INTEGER)""")

    if not cursor.execute("SELECT * FROM products").fetchall():
        with open(PRODUCTS_FILE, "r", encoding="utf-8") as file:
            for item in file:
                i = item.strip().strip("(").strip(")").strip("\n").replace(" ","").split(',')
                cursor.execute(f"INSERT INTO products (productname, type, pricetype, price, quantity) VALUES({i[0]},{i[1]},{i[2]},{i[3]},{i[4]})")
                db.commit()

    return cursor
