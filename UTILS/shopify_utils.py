import requests
from UTILS.proxy_utils import load_proxies


proxies = load_proxies()

def add_to_cart(shopify_store_url, variant_id, quantity, proxy):
    """Attempt to add a specific quantity of a product variant to the cart using a specific proxy."""

    with requests.Session() as session:
        session.proxies = {'http': proxy, 'https': proxy}
        add_to_cart_url = f'{shopify_store_url}/cart/add.js'
        payload = {'items': [{'id': variant_id, 'quantity': quantity}]}
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        response = session.post(add_to_cart_url, json=payload, headers=headers)
        return response.status_code == 200
    


def find_max_quantity(domain, variant_ids, max_limit=50000):
    """Find the maximum quantity of multiple product variants that can be added to the cart."""
    results = {}
    proxy_index = 0
    for variant_id in variant_ids:
    
        current_proxy = proxies[proxy_index % len(proxies)]
        proxy_index += 1

        # Test if adding one item is successful
        if not add_to_cart(domain, variant_id, 1, current_proxy):
            results[variant_id] = 0
            continue

        low = 1
        high = 2
        # Gradually increase 'high' to find a boundary
        while high <= max_limit:
            if add_to_cart(domain, variant_id, high, current_proxy):
                low = high
                high *= 2
                if high > max_limit:
                    high = max_limit
            else:
                break

        # Binary search between low and high
        while low < high - 1:
            mid = (low + high) // 2
            if add_to_cart(domain, variant_id, mid, current_proxy):
                low = mid
            else:
                high = mid

        results[variant_id] = low



    return results