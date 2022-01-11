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
    def __init__(self, id, topping, supplier, location):
        self.id = id
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
       # print ("topping: ?",[topping])
        # get a hat_id and it's supplier_id if the hat is topping = topping and quantity > 0
        c.execute("""
            SELECT id, supplier FROM hats  WHERE topping = ? AND quantity > 0 GROUP BY supplier ORDER BY supplier
        """, [topping])

        pair = c.fetchone()
       # print ("c fetched:",pair)
        # check if exists a hat that conforms the request
        if pair == None:
            return -1

        id = pair[0]
        supplier_id = pair[1]


        # update hats at id to have 1 less quantity
        c.execute("""
            UPDATE hats SET quantity = ? WHERE  id = ?
        """, [_Hats.find(self,id).quantity-1 , id])

        c.execute("""
            DELETE FROM hats WHERE id = ? AND quantity = 0
        """,[id])

        return [id,supplier_id]


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
  #      print("insert",stat.id,stat.topping,stat.supplier,stat.location,"to stats")
        self._conn.execute("""
            INSERT INTO stats (id, topping, supplier, location) VALUES (?, ?, ?, ?)
        """, [stat.id, stat.topping, stat.supplier, stat.location])

    def get_all(self):
        c = self._conn.cursor()
        all = c.execute("""
            SELECT id, topping, supplier, location FROM stats
        """).fetchall()
        return [Stat(*row) for row in all]


# The Repository
class _Repository:
    def __init__(self, db_path):
        self._conn = sqlite3.connect(db_path)
        self.hats = _Hats(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.orders = _Orders(self._conn)
        self.stats = _Stats(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.execute("""
        CREATE TABLE suppliers (
            id                 INT     PRIMARY KEY,
            name     VARCHAR    NOT NULL
        )""")

        self._conn.execute("""
        CREATE TABLE hats (
            id      INT         PRIMARY KEY,
            topping    VARCHAR        NOT NULL,
            supplier INT        NOT NULL,
            quantity INT        NOT NULL,
            
            FOREIGN KEY (supplier)      REFERENCES suppliers(id)
        )""")

        self._conn.execute("""
              CREATE TABLE orders (
                  id      INT         PRIMARY KEY,
                  location    VARCHAR        NOT NULL,
                  hat INT        NOT NULL,

                  FOREIGN KEY (hat)      REFERENCES hats(id)
              )""")

        self._conn.execute("""
              CREATE TABLE stats (
                  id      INT         PRIMARY KEY,
                  topping    VARCHAR        NOT NULL,
                  supplier VARCHAR        NOT NULL,
                  location VARCHAR NOT NULL

              )""")
        

    def get_available_supplier(self, topping_name):
        [hat_id,supplier_id] = self.hats.get_available_supplier(topping_name)
        if [hat_id,supplier_id] == [-1,-1] :
            return None
        supplier_name = self.suppliers.find(supplier_id).name
     #   self.hats.updateinventory()
        return [hat_id,supplier_name]



# Application logic
# library to read from path in os
import os





# main: read the config and then the orders
import sys
import os

def main (args):

    # restart db
    os.remove(args[4])

    # args: mainorsmth [config.txt] [orders.txt] [output.txt] [database.db]
    # C:\Caze Mattan\University\2nd year\3rd semester\courses\SPL\Project4\207382581-204867568\config.txt
    config_txt = args[1]
    orders_txt = args[2]
    output_txt = args[3]
    database_db = args[4]


    # initiate the repository singleton with said db
    repo = _Repository(database_db)

#    repo.__init__(database_db)
    # create tables
    repo.create_tables()

    # lines to check if file exists:     import os; os.path.isfile(filename);

    # configurate tables
    # save config file lines in array
    bad_lines = []
    lines = []
    with open(config_txt) as inputfile:
        for line in inputfile:
            lines.append(line)

    for i in range(len(lines)-1):
        lines[i] = lines[i][:len(lines[i])-1]


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
        hat_info[3] = hat_info[3][:2]
#        print ("quantity: ",hat_info[3],".")
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
    order_lines = []
    with open(orders_txt) as inputfile:
        for line in inputfile:
            order_lines.append(line)

    for i in range(len(order_lines) - 1):
        order_lines[i] = order_lines[i][:len(order_lines[i]) - 1]

    for line in order_lines:
        # create order from line
        order_location = line.split(",")[0]
        order_topping = line.split(",")[1]
        # try to get a supplier name and update quantity (execute order for topping)
        pair = repo.get_available_supplier(order_topping)
        hat_id = pair[0]
        supplier_name = pair[1]
        # if got a supplier then order was executed
        if (supplier_name != None):
            order = Order(order_id,order_location, hat_id)
            # add it's order to repository's orders db
            repo.orders.insert(order)
            order_id = order_id + 1
            # add stat of executed order to stats
            #   print ("order topping",order_topping,"supplier name",supplier_name,"order location",order_location)
            stat = Stat(order_id, order_topping, supplier_name, order_location)
            repo.stats.insert(stat)

    summary = repo.stats.get_all()
    os.remove(output_txt)
    with open(output_txt, "x") as outputfile:
        for stat in summary:
            summary_stat = stat.topping + "," + stat.supplier + "," + stat.location + '\n'
            outputfile.write(summary_stat)
    #close repository
    repo._close()

if __name__ == '__main__':
    main(sys.argv)