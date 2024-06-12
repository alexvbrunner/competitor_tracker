from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor, as_completed
from src.data_processing import *
from UTILS.db_operations import *
from UTILS.shopify_utils import *
from UTILS.utils import *
from datetime import datetime
import time
from multiprocessing import cpu_count
import tqdm


def process_row(row, already_processed_ids):
    """
    Processes a single row of product data. Checks if the product has already been processed,
    attempts to find a non-zero quantity of the product, and if found, adds it to the set of processed IDs.

    Args:
    row (tuple): Contains product_id, product_name, price, and domain.
    already_processed_ids (set): Set of product IDs that have already been processed.

    Returns:
    tuple or None: Returns a tuple with product details and quantity if a non-zero quantity is found,
                   otherwise returns None.
    """

    product_id, product_name, price, domain = row

    if str(product_id) in already_processed_ids:    
        return None
  
    attempts = 0
    max_attempts = 2
    qty = 0

    # Try up to two times to find a non-zero quantity
    while attempts < max_attempts and qty == 0:
        max_qty = find_max_quantity(domain, [product_id])
        qty = max_qty.get(product_id, 0)
        attempts += 1
        if qty == 0:
            time.sleep(5)

    if qty == 0:
        return None
    else:
       
        already_processed_ids.add(str(product_id)) 
        return (product_id, product_name, price, domain, qty, datetime.now())




def handle_future_result(future, low_quantity_ids, cur, target_table, data_to_insert, data_to_update):
    try:
        result = future.result()
        if result:
            product_id = result[0]
            if product_id in low_quantity_ids:
                cur.execute(f"SELECT Quantity FROM {target_table} WHERE Product_ID = %s", (product_id,))
                old_qty = cur.fetchone()[0]
                if old_qty != result[4]:
                    data_to_update.append(result)
            else:
                data_to_insert.append(result)
            return True
    except Exception as e:
        print(f"An error occurred processing a row: {e}")
    return False




def process_rows_concurrently(rows, already_processed_ids):
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        futures = {executor.submit(process_row, row, already_processed_ids): row for row in rows}
        return futures



def handle_results(futures, low_quantity_ids, cur, target_table, conn):
    
    data_to_insert = []
    data_to_update = []
    results_count = 0
    processed_count = 0
    start_time = time.time()

    for future in tqdm(as_completed(futures), total=len(futures), desc="Processing products"):
        result = handle_future_result(future, low_quantity_ids, cur, target_table, data_to_insert, data_to_update)
        if result:
            results_count += 1
            processed_count += 1

            check_rate_limit(processed_count, start_time)
            if results_count >= 5:
                batch_insert_to_database(conn, data_to_insert, target_table)
                update_database(conn, data_to_update, target_table)
                data_to_insert = []
                data_to_update = []
                results_count = 0

    return data_to_insert, data_to_update


def process_data_for_all_domains(domain_tables):
    """
    Processes data for all domain tables listed. Uses a ProcessPoolExecutor to handle multiple domain tables concurrently.

    Args:
    domain_tables (list): List of domain-specific table names to process.

    Returns:
    None
    """
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = []
        for domain_table in domain_tables:
            future = executor.submit(process_domain_table, domain_table)
            futures.append(future)

        for future in as_completed(futures):

            try:
                # Optionally handle the result or exceptions from each future
                result = future.result()
                print(f"Completed processing for a domain table: {result}")
            except Exception as e:
                print(f"An error occurred during processing: {e}")



def process_domain_table(domain_table):
    """
    Processes a single domain table. Establishes a database connection, checks and creates necessary tables,
    and processes the data.

    Args:
    domain_table (str): Name of the domain-specific table to process.

    Returns:
    str: Name of the domain table processed.
    """

    conn = connect_to_mysql_db()  # Create a new connection for each process
    try:
        new_table_name = create_qty_table_if_not_exists(conn, domain_table)
        fetch_and_process_data(conn, domain_table, new_table_name)
    finally:
        conn.close()  # Ensure the connection is closed after processing
    return domain_table



def fetch_and_process_data(conn, domain_table, target_table):
    """
    Fetches product data from a specified domain table and processes it to update or insert into a target table.
    This function ensures the target table exists, fetches rows from the domain table, processes them concurrently,
    and then updates the database with new or modified entries.

    Args:
    conn (mysql.connector.connection.MySQLConnection): The database connection object.
    domain_table (str): The name of the table containing domain-specific product data.
    target_table (str): The name of the table where processed data will be stored or updated.

    Returns:
    None
    """
    ensure_table_exists(conn, target_table)
    cur = conn.cursor()
    now = datetime.now()
    start_of_day = datetime(now.year, now.month, now.day)
    already_processed_ids, low_quantity_ids = fetch_already_processed_ids(cur, target_table, start_of_day, now)
    rows = fetch_product_rows(cur, domain_table)
    futures = process_rows_concurrently(rows, already_processed_ids)
    data_to_insert, data_to_update = handle_results(futures, low_quantity_ids, cur, target_table, conn)
    if data_to_insert:
        batch_insert_to_database(conn, data_to_insert, target_table)
    if data_to_update:
        update_database(conn, data_to_update, target_table)