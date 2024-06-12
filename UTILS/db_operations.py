import mysql.connector

def connect_to_mysql_db():
    """
    Connect to a MySQL database.
    """
    try:
        conn = mysql.connector.connect(
            database='competitor_tracker_db',
            user='maniko',
            password='Heslo0987*',
            host='89.116.111.122',
        )
        print("MySQL DB connection established.")
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL DB: {e}")
        return None  

def ensure_table_exists(conn, table_name):
    cur = conn.cursor()
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            Product_ID VARCHAR(255),
            Product_Name VARCHAR(255),
            Price DECIMAL(10, 2),
            Domain VARCHAR(255),
            Quantity INT,
            Date TIMESTAMP
        )
    ''')
    conn.commit()


def batch_insert_to_database(conn, data, table_name):
    cur = conn.cursor()
    insert_query = f'''
        INSERT INTO {table_name} (Product_ID, Product_Name, Price, Domain, Quantity, Date)
        VALUES (%s, %s, %s, %s, %s, %s)
    '''
    cur.executemany(insert_query, data)
    conn.commit()


def update_database(conn, data, table_name):
    cur = conn.cursor()
    update_query = f'''
        UPDATE {table_name}
        SET Product_Name = %s, Price = %s, Domain = %s, Quantity = %s
        WHERE Product_ID = %s AND DATE(Date) = DATE(%s)
    '''
    # Prepare data for update (note the order of fields should match the SQL query)
    data_for_update = [(item[1], item[2], item[3], item[4], item[5], item[0]) for item in data]
    cur.executemany(update_query, data_for_update)
    conn.commit()


def ensure_table_exists_with_structure(conn, table_name):
    cur = conn.cursor()
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            Product_ID VARCHAR(255),
            Product_Name VARCHAR(255),
            Price DECIMAL(10, 2),
            Domain VARCHAR(255),
            Quantity INT,
            Date TIMESTAMP
        )
    ''')
    conn.commit()


def create_qty_table_if_not_exists(conn, domain_table):
    # Format the new table name by replacing 'https' with 'https_' and 'products' with 'qty'
    new_table_name = domain_table.replace(
        'https', 'https_').replace('products', 'qty')
    ensure_table_exists_with_structure(conn, new_table_name)
    print(f"Checked or created table: {new_table_name}")
    return new_table_name


def fetch_already_processed_ids(cur, target_table, start_of_day, now):
    cur.execute(f"SELECT Product_ID, Quantity FROM {target_table} WHERE Date >= %s AND Date < %s", (start_of_day, now))
    already_processed_ids = {str(row[0]) for row in cur.fetchall()}
    low_quantity_ids = {str(row[0]) for row in cur.fetchall() if row[1] < 10}
    return already_processed_ids, low_quantity_ids

def fetch_product_rows(cur, domain_table):
    cur.execute(f"SELECT Product_ID, Product_Name, Price, Domain FROM {domain_table}")
    return cur.fetchall()