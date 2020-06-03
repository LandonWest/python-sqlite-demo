import logging
import sqlite3
from sqlite3 import Error

db = sqlite3.connect(':memory:')  # using in-memory db
cur = db.cursor()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sql_connection():
    try:
        con = sqlite3.connect(':memory:')
        logger.info('Connection established: Database created in memory')
        return con
    except Error as e:
        raise('Connecting to the db failed, ', e)


# Create the Database Tables

def create_customer_table(connection):
    cur = connection.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS Customer (
           id integer PRIMARY KEY,
           firstname varchar(255),
           lastname varchar(255) )"""
    )
    connection.commit()


def create_item_table(connection):
    cur = connection.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS Item (
           id integer PRIMARY KEY,
           title varchar(255),
           price decimal )"""
    )
    connection.commit()


def create_purchase_table(connection):
    cur = connection.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS Purchase (
           ordernumber integer PRIMARY KEY,
           customerid integer,
           itemid integer,
           price decimal,
           CONSTRAINT customerid
               FOREIGN KEY (customerid) REFERENCES Customer(id),
           CONSTRAINT itemid
               FOREIGN KEY (itemid) REFERENCES Item(id) )"""
    )
    connection.commit()


# Populate the Tables with Data

def insert_into_table(connection, table, column_names, data):
    """Inserts data into the specified database table

    param: connection: sqlite3.connect() object
    param: table: str of table name, ex. 'Customer'
    param: column_names: tuple of str, ex. ('firstname', 'lastname')
    param: data: tuple of str or a list of tuple of str. ex. ('John', 'Smith')

    Example:
    cur.execute(
        '''INSERT INTO Item(title, price)
           VALUES ('USB', 10.2),
                  ('Mouse', 12.23),
                  ('Monitor', 199.99);'''
    )

    returns: None
    """
    cur = connection.cursor()
    bindings = tuple(['?' for col_name in column_names])
    statement = f'INSERT INTO {table}{column_names} VALUES {bindings}'.replace("'", "")
    logger.info(statement)
    logger.info(data)

    if isinstance(data, tuple):
        cur.execute(statement, data)
    if isinstance(data, list):
        cur.executemany(statement, data)

    connection.commit()


# Aggregation Functions

# AVG
def get_avg(connection):
    cur = connection.cursor()
    cur.execute('''SELECT itemid, AVG(price) FROM Purchase GROUP BY itemid''')
    result = cur.fetchall()
    logger.info('average paid price by item id: ')
    logger.info(result)
    return result


def get_avg_with_name(connection):
    cur = connection.cursor()
    cur.execute(
        '''SELECT item.title, AVG(purchases.price) FROM Purchase as purchases
           INNER JOIN Item as item on (item.id = purchases.itemid)
           GROUP BY purchases.itemid'''
    )
    result = cur.fetchall()
    logger.info('average paid price by item name: ')
    logger.info(result)
    return result


# SUM
def get_sum(connection):
    cur = connection.cursor()
    cur.execute(
        '''SELECT customer.firstname, SUM(purchases.price) FROM Purchase as purchases
           INNER JOIN Customer as customer on (customer.id = purchases.customerid)
           GROUP BY customer.firstname'''
    )
    result = cur.fetchall()
    logger.info('sum of purchase prices by customer: ')
    logger.info(result)
    return cur.fetchall()


# Debugging SQL Queries
def explain_query(connection):
    cur = connection.cursor()
    cur.execute(
        '''EXPLAIN QUERY PLAN SELECT customer.firstname, item.title, 
           item.price, purchases.price FROM Purchase as purchases
           INNER JOIN Customer as customer on (customer.id = purchases.customerid)
           INNER JOIN Item as item on (item.id = purchases.itemid)'''
    )
    result = cur.fetchall()
    logger.info('explaining query...')
    logger.info(result)
    return result


if __name__ == '__main__':
    con = sql_connection()
    create_customer_table(con)
    create_item_table(con)
    create_purchase_table(con)

    customers_data = [('John', 'Smith'), ('Amy', 'Adams'), ('Jim', 'Beam')]
    insert_into_table(con, 'Customer', ('firstname', 'lastname'), customers_data)

    items_data = [('MacBook Pro', 3199.99), ('MacBook Air', 1188.99), ('iPad', 599.99)]
    insert_into_table(con, 'Item', ('title', 'price'), items_data)

    purchases_data = [(1, 1, 3099.99), (1, 2, 1202.23), (1, 3, 599.99), (2, 3, 580.00), (3, 2, 1188.99)]
    insert_into_table(con, 'Purchase', ('customerid', 'itemid', 'price'), purchases_data)

    get_avg(con)
    get_avg_with_name(con)
    get_sum(con)
    explain_query(con)
