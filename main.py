# Data Transfer Objects:
import sqlite3


class Hat:
    def __init__(self, id, topping, supplier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat

class Stat:
    def __init__(self, topping, supplier, location):
        self.topping = topping
        self.supplier = supplier
        self.location = location

# Data Access Objects:
# All of these are meant to be singletons
class _Hats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, hat):
        self._conn.execute("""
               INSERT INTO hats (id, topping, supplier, quantity) VALUES (?, ?, ?, ?)
           """, [hat.id, hat.topping, hat.supplier, hat.quantity])

    def find(self, hat_id):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, topping, supplier, quantity FROM hats WHERE id = ?
        """, [hat_id])

        return Hat(*c.fetchone())

    # delete for when quantity is 0
    def delete(self, hat_id):
        c = self._conn.cursor()
        c.execute("""
            DELETE FROM hats WHERE id = ?
        """, [hat_id])

    def updateinventory(self):
        c = self._conn.cursor()
        c.execute("""
            DELETE FROM hats WHERE quantatiy = 0
        """)

    def get_available_supplier(self, topping):
        c = self._conn.cursor()

        # get a hat_id and it's supplier_id if the hat is topping = topping and quantity > 0
        c.execute("""
            SELECT id, supplier FROM hats WHERE topping = ? AND quantity > 0
        """, [topping])
        [id,supplier_id] = c.fetchone

        # check if exists a hat that conforms the request
        if id == None or supplier_id == None:
            return -1

        # update hats at id to have 1 less quantity
        c.execute("""
            UPDATE hats SET quantity = ? WHERE  id = ?
        """, [_Hats.find(id).quantity-1 , id])

        return supplier_id


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
                INSERT INTO suppliers (id, name ) VALUES (?, ?)
        """, [supplier.id, supplier.name])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT id,name FROM suppliers WHERE id = ?
            """, [id])

        return Supplier(*c.fetchone())


class _Orders:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, order):
        self._conn.execute("""
                INSERT INTO orders (id, location, hat ) VALUES (?, ?, ?)
        """, [order.id, order.location, order.hat])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT id,location,hat FROM orders WHERE id = ?
            """, [id])

        return Order(*c.fetchone())

class _Stats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, stat):
        self._conn.execute("""
            INSERT INTO stats (topping, supplier, location) VALUES (?, ?, ?)
        """, [stat.topping, stat.supplier, stat.location])

    def get_all(self):
        c = self._conn.cursor()
        all = c.execute("""
            SELECT stat.topping, stat.supplier, stat.location FROM stats
        """).fetchall()
        return [Stat(*row) for row in all]


# The Repository
class _Repository:
    def __init__(self):
        self._conn = sqlite3.connect('grades.db')
        self.hats = _Hats(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.orders = _Orders(self._conn)
        self.stats = _Stats(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE hats (
            id      INT         PRIMARY KEY,
            topping    TEXT        NOT NULL,
            supplier INT        NOT NULL,
            quantity INT        NOT NULL,
            
            FOREIGN KEY (supplier)      REFERENCES suppliers(id)
        );

        CREATE TABLE suppliers (
            id                 INT     PRIMARY KEY,
            name     TEXT    NOT NULL
        );

        CREATE TABLE orders (
            id      INT     PRIMARY KEY ,
            location  TEXT     NOT NULL,
            hat           INT     NOT NULL,

            FOREIGN KEY(hat)     REFERENCES hats(id),
        );
        
        CREATE TABLE stats (
            topping      TEXT     NOT NULL ,
            supplier  TEXT     NOT NULL,
            location           TEXT     NOT NULL,
            
            
            PRIMARY  KEY (topping,supplier,location)
        );
    """)

    def get_available_supplier(self, topping_name):
        supplier_id = self.hats.get_available_supplier(topping_name)
        if supplier_id == -1 :
            return None
        supplier_name = self.suppliers.find(supplier_id).name
        self.hats.updateinventory()
        return supplier_name

# the repository singleton
repo = _Repository()


# Application logic
# library to read from path in os
import os





# main: read the config and then the orders
import sys

def main (args):
    # initiate repository
    repo.__init__()
    # create tables
    repo.create_tables()

    # args: mainorsmth [config.txt] [orders.txt] [output.txt] [database.db]
    configtext = args[1]
    orderdstxt = args[2]
    outputtxt = args[3]
    # how to use database db?
    databasedb = args[4]
    # lines to check if file exists:     import os; os.path.isfile(filename);

    # configurate tables
    # save config file lines in array
    lines = []
    with open(configtext) as inputfile:
        for line in inputfile:
            lines.append(line)
    # first line is num of hats and num of suppliers seperated with ','
    nums = lines[0].split(",")
    num_of_hats = int(nums[0])
    num_of_suppliers = int(nums[1])
    # deduct first line to symbol it has been read
    lines = lines[1:]
    # add hats
    for i in range(num_of_hats):
        hat_info = lines[0].split(",")
        lines = lines[1:]
        hat_id = int(hat_info[0])
        topping = hat_info[1]
        supplier_id = int(hat_info[2])
        quantity = int(hat_info[3])
        hat = Hat(hat_id,topping,supplier_id,quantity)
        repo.hats.insert(hat)
    # add suppliers
    for i in range(num_of_suppliers):
        supplier_info = lines[0].split(",")
        lines = lines[1:]
        supplier_id = int(supplier_info[0])
        supplier_name = supplier_info[1]
        supplier = Supplier(supplier_id, supplier_name)
        repo.suppliers.insert(supplier)



    # execute orders
    # make orders id
    order_id = 1
    with open(orderdstxt) as inputfile:
        for line in inputfile:
            # create order from line
            order_location = line.split(",")[0]
            order_topping = line.split(",")[1]
            order = Order(order_id,order_location, order_topping)
            # try to get a supplier name and update quantity (execute order for topping)
            supplier_name = repo.get_available_supplier(order_topping)
            # if got a supplier then order was executed
            if (supplier_name != None):
                # add it's order to repository's orders db
                repo.orders.insert(order)
                order_id = order_id + 1
                # add stat of executed order to stats
                stat = Stat(order_topping,supplier_name, order_location)
                repo.stats.insert(stat)
    summary = repo.stats.get_all()
    with open(outputtxt) as outputfile:
        for stat in summary:
            summary_stat = stat.topping + "," + stat.supplier + "," + stat.location
            outputfile.write(summary_stat)

    #close repository
    repo._close()


if __name__ == '__main__':
    main(sys.argv)