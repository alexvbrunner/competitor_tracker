import requests
import xml.etree.ElementTree as ET
import sqlite3
import mysql
from UTILS.server_utils import connect_to_mysql_db


# List of domains to scrape
domains = [
    "https://www.moyou.co.uk",
]


def create_table(domain):
    table_name = f"{domain.replace('.', '_').replace('-', '_').replace('/', '_').replace(':', '_')}_products"
    conn = connect_to_mysql_db()
    cursor = conn.cursor()
    # Create the table if it does not exist
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            Product_Name TEXT,
            Product_ID BIGINT,  # Changed to BIGINT to accommodate larger numbers
            Price REAL,
            Domain TEXT
        )
    ''')
    # Check if the Product_ID column is of the correct type and alter if necessary
    cursor.execute(f'''
        SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table_name}' AND COLUMN_NAME = 'Product_ID';
    ''')
    result = cursor.fetchone()
    if result and result[0] != 'BIGINT':
        cursor.execute(f'''
            ALTER TABLE {table_name} MODIFY COLUMN Product_ID BIGINT;
        ''')
    conn.commit()
    conn.close()
    return table_name


def store_product_data(table_name, product_name, product_id, price, domain):
    conn = connect_to_mysql_db()
    cursor = conn.cursor()
    try:
        if product_id is not None:
            cursor.execute(f"INSERT INTO {table_name} (Product_Name, Product_ID, Price, Domain) VALUES (%s, %s, %s, %s)",
                           (product_name, int(product_id), price, domain))
            conn.commit()
        else:
            print(
                f"Skipping insertion for {product_name} due to invalid Product_ID.")
    except ValueError:
        print(f"Invalid Product_ID {product_id} for {product_name}.")
    except mysql.connector.errors.DataError as e:
        print(
            f"DataError: {e}. Check if Product_ID {product_id} is out of range.")
    finally:
        conn.close()


def extract_products(domain, table_name):
    response = requests.get(f"{domain}/sitemap.xml")
    root = ET.fromstring(response.content)

    for sitemap in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap'):
        loc = sitemap.find(
            '{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text

        if 'sitemap_products' in loc:
            product_response = requests.get(loc)

            product_root = ET.fromstring(product_response.content)

            for url in product_root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                product_link = url.find(
                    '{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text

                # Check if 'products' is in the URL
                if 'products' in product_link:
                    # Assuming product_link is a URL that returns XML content
                    product_page = requests.get(f"{product_link}.xml")

                    product_tree = ET.fromstring(product_page.content)
                    print(product_tree)
                    title = product_tree.find('.//title').text
                    # Assuming the XML has a structure with a variant that includes an ID

                    variant = product_tree.find('.//variant')
                    if variant is not None:
                        product_id = variant.find(
                            './/id[@type="integer"]').text
                        price = variant.find('.//price').text
                    else:
                        product_id = None  # or handle the absence of a variant as needed
                        price = None
                    store_product_data(table_name, title,
                                       product_id, price, domain)


def main():
    for domain in domains:
        table_name = create_table(domain)  # Create table for each domain
        print(table_name)
        # Pass the table_name to the function
        extract_products(domain, table_name)


if __name__ == "__main__":
    main()



