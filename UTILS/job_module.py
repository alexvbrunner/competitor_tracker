import time
from UTILS.db_operations import *
from UTILS.shopify_utils import *
from src.data_processing import *

def main():
    """
    Main function to handle the processing of domain-specific tables. Manages database connection attempts and
    processes all domain tables in a loop. Implements a delay mechanism to handle rate limits or operational constraints.
    """
    domain_tables = [
        'https___uvnailz_com__products',
        'https___www_doonails_com__products',
        'https___colour_flash_com_products',
        'https___pinkypact_com__products',
        'https___gellae_com_au_products',
        'https___polishpops_com_products',
        'https___nailuxe_de_products',
        'https___ohora_com_products',
        'https___gelmore_de_products',
        'https___dashingdiva_com_products',
        'https___www_miss_sophie_com__products',
        'https___bimanails_com_products',
        'https___blitsbee_com_products',
        'https___gel_nail_wrap_nl_products',
        'https___gimeau_com_products',
        'https___jelcie_com_products',
        'https___maniac_nails_com_products',
        'https___www_moyou_co_uk_products',
        'https___riminibeauty_co_uk__products',
        'https___sheesh_beauty_com_products',
        'https___www_dannitoni_com_products',
        'https___www_fairy_nail_de__products',
        'https___www_folienschwestern_de__products',
    ]

    max_connection_attempts = 3
    attempt = 0
    while True:
        try:
            process_data_for_all_domains(domain_tables)
            print('All processed')
            time.sleep(3600)  # Sleep for 1 hour (3600 seconds)
        except mysql.connector.errors.OperationalError as e:
            if attempt < max_connection_attempts:
                print(f"Connection attempt {attempt + 1} failed. Retrying...")
                attempt += 1
                time.sleep(10)
            else:
                print("Failed to connect to the database after several attempts.")
                break
        else:
            attempt = 0  # Reset attempt counter after successful connection

def job():
    main()
    #main