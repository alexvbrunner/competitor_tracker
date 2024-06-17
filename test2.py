import time
from UTILS.shopify_utils import add_to_cart,find_max_quantity

def load_proxies():
    """Load and format proxies with credentials for rotating proxies."""
    username = "qfkdohpl-rotate"
    password = "rdohpw8rodyc"
    domain = "p.webshare.io"
    port = 80
    formatted_proxy = f"http://{username}:{password}@{domain}:{port}"
    return [formatted_proxy]




def main():
    shopify_store_url = "https://bonnieplants.com/"
    variant_ids = ["39998609522739"]
    proxies = load_proxies()
    proxy = proxies[0]

    max_quantities = {}

    for variant_id in variant_ids:
        low = 1  # Minimum quantity to start testing
        high = None  # We don't know the max yet, so start with None
        last_successful_qty = 0
        max_qty=find_max_quantity(shopify_store_url,variant_ids,max_limit=50000)
        print(max_qty)

    # Print the results
    for variant_id, max_qty in max_quantities.items():
        print(f"Max quantity for {variant_id}: {max_qty}")

if __name__ == '__main__':
    main()

##43009734803643: 96
#
#Max quantity for 45004539265290: 794
#Max quantity for 34413336658060: 973
#Max quantity for 40385374355596: 824

#{37798535495867: 2048}
#{37798506725563: 2048}

##{46545936646482: 24576}
#{46545805640018: 24576}
