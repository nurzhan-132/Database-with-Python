import sqlite3
from sqlite3 import Error
import time, os
from bottle import route, run, template, request, redirect

class DatabaseApplication:

    def __init__(self, _customized_path, _database_file_name, _database_name):
        self._customized_path = _customized_path
        self._database_file_name = _database_file_name
        self._database_name = _database_name
        self.full_database_path = self._customized_path + self._database_file_name
        self.index_html = self.return_default_page()

    def return_default_page(self):
        index_html = ""
        with open(self._customized_path + "index.html", 'r') as input_f:
            for line in input_f:
                index_html += line
        return index_html

    def openConnection(self, _dbFile):
        print("++++++++++++++++++++++++++++++++++")
        print("Open database: ", _dbFile)
        conn = None
        try:
            conn = sqlite3.connect(_dbFile)
            print("success")
        except Error as e: print(e)
        print("++++++++++++++++++++++++++++++++++")
        return conn

    def closeConnection(self, _conn, _dbFile):
        print("++++++++++++++++++++++++++++++++++")
        print("Close database: ", _dbFile)
        try:
            _conn.close()
            print("success")
        except Error as e: print(e)
        print("++++++++++++++++++++++++++++++++++")

    def queryAllTableNames(self, _conn):
        try:
            start = time.time()
            sql = """SELECT name FROM sqlite_master WHERE type='table';"""
            cur = _conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            print("success data retrieval")
            return rows, performance
        except Error as e: print(e)
        return None, None

    def queryAllTableContent(self, _conn, table_name):
        try:
            start = time.time()
            sql = """select * from """ + table_name + """;"""
            cur = _conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            print("success data retrieval")
            return rows, performance, cur.description
        except Error as e: print(e)
        return None, None, None

    ########## 1.sql ########## 
    def testQuery1(self, _conn):
        print("testQuery1")
        try:
            start = time.time()
            sql = """SELECT c_address, c_phone, c_acctbal 
                        FROM customer 
                            WHERE c_name = 'Customer#000000129';"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["c_address"], ["c_phone"], ["c_acctbal"]]
        except Error as e: print(e)
        return None, None, None

    ########## 2.sql ##########
    def testQuery2(self, _conn):
        print("testQuery2")
        try:
            start = time.time()
            sql = """SELECT s_name, s_acctbal   
                        FROM supplier 
                            WHERE s_acctbal = (SELECT MAX(s_acctbal) FROM supplier)
                            ORDER BY s_name;"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["s_name"], ["s_acctbal"]]
        except Error as e: print(e)
        return None, None, None

    ########## 3.sql ##########
    def testQuery3(self, _conn):
        print("testQuery3")
        try:
            start = time.time()
            sql = """SELECT COUNT(DISTINCT p_name) 
                        FROM part
                            WHERE p_type LIKE '%STANDARD BURNISHED%' AND    
                            (p_size = 6 OR p_size = 23 OR p_size = 43);"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["COUNT(DISTINCT p_name)"]]
        except Error as e: print(e)
        return None, None, None

    ########## 4.sql ##########
    def testQuery4(self, _conn):
        print("testQuery4")
        try:
            start = time.time()
            sql = """SELECT COUNT(DISTINCT o_orderkey) 
                        FROM orders 
                            WHERE strftime('%Y', o_orderdate) = '1993';"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["COUNT(DISTINCT o_orderkey)"]]
        except Error as e: print(e)
        return None, None, None

    ########## 5.sql ##########
    def testQuery5(self, _conn):
        print("testQuery5")
        try:
            start = time.time()
            sql = """SELECT p_type, COUNT(p_type) 
                        FROM part 
                            GROUP BY p_type HAVING p_type LIKE '%NICKEL%' 
                            ORDER BY p_type ASC;"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["p_type"], ["COUNT(p_type)"]]
        except Error as e: print(e)
        return None, None, None

    ########## 6.sql ##########
    def testQuery6(self, _conn):
        print("testQuery6")
        try:
            start = time.time()
            sql = """SELECT ps_partkey, (ps_supplycost*ps_availqty)   
                        FROM partsupp
                            ORDER BY ps_supplycost*ps_availqty DESC     
                            LIMIT 10;"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["ps_partkey"], ["(ps_supplycost*ps_availqty)"]]
        except Error as e: print(e)
        return None, None, None

    ########## 7.sql ##########
    def testQuery7(self, _conn):
        print("testQuery7")
        try:
            start = time.time()
            sql = """SELECT l_orderkey, MAX(l_extendedprice*(1-l_discount)) 
                        FROM lineitem
                            WHERE strftime('%Y %m %d', l_shipdate) <> '1994 11 28'
                            ORDER BY l_orderkey ASC;"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["l_orderkey"], ["l_extendedprice*(1-l_discount)"]]
        except Error as e: print(e)
        return None, None, None

    ########## 8.sql ##########
    def testQuery8(self, _conn):
        print("testQuery8")
        try:
            start = time.time()
            sql = """SELECT ROUND(SUM(p_retailprice)) 
                        FROM part 
                            WHERE p_retailprice < 2000 AND 
                            (p_mfgr='Manufacturer#1' OR p_mfgr='Manufacturer#2' OR p_mfgr='Manufacturer#3' OR p_mfgr='Manufacturer#4' OR p_mfgr='Manufacturer#5' OR p_mfgr='Manufacturer#6');"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["ROUND(SUM(p_retailprice))"]]
        except Error as e: print(e)
        return None, None, None

    ########## 9.sql ##########
    def testQuery9(self, _conn):
        print("testQuery9")
        try:
            start = time.time()
            sql = """SELECT c_mktsegment, MIN(c_acctbal), MAX(c_acctbal), AVG(c_acctbal), SUM(c_acctbal)
                        FROM customer
                            GROUP BY c_mktsegment
                            ORDER BY SUM(c_acctbal) DESC;"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["c_mktsegment"], ["MIN(c_acctbal)"], ["MAX(c_acctbal)"], ["AVG(c_acctbal)"], ["SUM(c_acctbal)"]]
        except Error as e: print(e)
        return None, None, None

    ########## 10.sql ##########
    def testQuery10(self, _conn):
        print("testQuery10")
        try:
            start = time.time()
            sql = """SELECT strftime('%m', l_shipdate) AS per_month, SUM(l_quantity) AS tot_month
                         FROM lineitem
                             WHERE strftime('%Y', l_shipdate) = '1996'
                             GROUP BY strftime('%m', l_shipdate);"""
            cur = _conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            end = time.time()
            performance = str(end - start)
            return result, performance, [["per_month"], ["total_month"]]
        except Error as e: print(e)
        return None, None, None


"""
    The global variables that need to be correctly set.
"""
folder_path = ""  # path to the database file
db_file_name = "tpch.sqlite"  # database file
db_name = 'TPC-H'  # database name
db_application = DatabaseApplication(folder_path, db_file_name, db_name)

"""
    Initial page.
"""
@route('/', method=['GET', 'POST'])
@route('/refresh', method=['GET', 'POST'])
def index():
    conn = db_application.openConnection(db_application.full_database_path)
    result, performance = None, None
    with conn:
        result, performance = db_application.queryAllTableNames(conn)
    db_application.closeConnection(conn, db_application.full_database_path)
    return template(db_application.index_html, chosen_table='', db_name=db_application._database_name, 
                    tables=result, performance_value='')

"""
    Function that retreives all the table rows.
"""
@route('/analysis', method=['GET', 'POST'])
def table_details():
    chosen_table_name = request.GET.get('all_tables')
    if not chosen_table_name: chosen_table_name = request.GET.get('chosen_table_name')
    if chosen_table_name:
        conn = db_application.openConnection(db_application.full_database_path)
        result, performance, columns = None, None, None
        with conn:
            result, performance, columns = db_application.queryAllTableContent(conn, chosen_table_name)
        db_application.closeConnection(conn, db_application.full_database_path)
        return template(db_application.index_html, chosen_table=chosen_table_name, db_name=db_application._database_name, 
                        tables=[[chosen_table_name]], table_content=result, table_columns=columns, 
                        performance_value=performance, query_performance='')
    return redirect('/')

"""
    Function that handles data retrievals from the database.
"""
@route('/retrieve', method=['GET', 'POST'])
def retrieve_data():
    conn = db_application.openConnection(db_application.full_database_path)
    result, performance, columns = None, None, None
    chosen_table_name = request.GET.get('chosen_table_name')

    query_number = request.GET.get("queryNumber")
    with conn:
        if query_number == "Lab3: Query1": result, performance, columns = db_application.testQuery1(conn)
        elif query_number == "Lab3: Query2": result, performance, columns = db_application.testQuery2(conn)
        elif query_number == "Lab3: Query3": result, performance, columns = db_application.testQuery3(conn)
        elif query_number == "Lab3: Query4": result, performance, columns = db_application.testQuery4(conn)
        elif query_number == "Lab3: Query5": result, performance, columns = db_application.testQuery5(conn)
        elif query_number == "Lab3: Query6": result, performance, columns = db_application.testQuery6(conn)
        elif query_number == "Lab3: Query7": result, performance, columns = db_application.testQuery7(conn)
        elif query_number == "Lab3: Query8": result, performance, columns = db_application.testQuery8(conn)
        elif query_number == "Lab3: Query9": result, performance, columns = db_application.testQuery9(conn)
        elif query_number == "Lab3: Query10": result, performance, columns = db_application.testQuery10(conn)
    db_application.closeConnection(conn, db_application.full_database_path)

    return template(db_application.index_html, chosen_table=chosen_table_name, db_name=db_application._database_name, 
                    tables=[[chosen_table_name]], table_content=result, table_columns=columns, 
                    performance_value='', query_performance=performance)

if __name__ == '__main__':
    address = "0.0.0.0"  # host address
    port = int(os.environ.get('PORT', 8080))  # port number
    run(host=address, port=port, debug=True)  # access at http://0.0.0.0:8080
