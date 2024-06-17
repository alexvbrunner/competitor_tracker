import requests
from UTILS.proxy_utils import load_proxies
import time
import threading
import time

# Global request counter, lock, and condition variable
request_counter = 0
request_limit = None  # Start with no predefined limit # Initial request limit per minute
counter_lock = threading.Lock()
cooldown_condition = threading.Condition()
cooldown_period = 300  # Cooldown period in seconds (5 minutes)
cooldown_multiplier = 0.7  # Multiplier to reduce the request limit
cooldown_extension = 60  # Time to extend cooldown period after each trigger

def handle_cooldown():
    global request_limit, cooldown_period
    with cooldown_condition:
        cooldown_condition.wait(timeout=cooldown_period)  # Wait for the cooldown period
        request_limit *= cooldown_multiplier  # Reduce the request limit by 30%
        cooldown_period += cooldown_extension  # Extend the cooldown period
        print(f"New adjusted request limit: {request_limit}")
        print(f"Cooldown period extended to: {cooldown_period} seconds")
        cooldown_condition.notify_all()

proxies = load_proxies()

def add_to_cart(shopify_store_url, variant_id, quantity, proxy):
    global request_counter, request_limit
    with counter_lock:
        if request_limit is not None and request_counter >= request_limit:
            print('Global limit reached, cooling down')
            handle_cooldown()
        request_counter += 1  # Increment the request counter safely within the lock

    try:
        with requests.Session() as session:
            session.proxies = {'http': proxy, 'https': proxy}
            add_to_cart_url = f'{shopify_store_url}/cart/add.js'
            payload = {'items': [{'id': variant_id, 'quantity': quantity}]}
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
            response = session.post(add_to_cart_url, json=payload, headers=headers)
            if response.status_code == 200:
                return (True, response.status_code, None)
            elif response.status_code == 429:
                if request_limit is None:  # First encounter of 429
                    with counter_lock:
                        request_limit = request_counter  # Set initial limit based on current counter
                        print(f"Initial request limit determined: {request_limit}")
                handle_cooldown()
                return (False, response.status_code, response.text)
            else:
                return (False, response.status_code, response.text)
    except requests.RequestException as e:
        return (False, None, str(e))

    


def find_max_quantity(domain, variant_ids, max_limit=50000):
    """Find the maximum quantity of multiple product variants that can be added to the cart."""
    results = {}
    proxy_index = 0
    for variant_id in variant_ids:
    
        current_proxy = proxies[proxy_index % len(proxies)]
        proxy_index += 1

        # Test if adding one item is successful
        success, status_code, error = add_to_cart(domain, variant_id, 1, current_proxy)
        if not success:
            if status_code == 422:
                continue  # Skip to the next variant_id if the error is 422
            results[variant_id] = 0
            continue

        low = 1
        high = 2
        # Gradually increase 'high' to find a boundary
        while high <= max_limit:
            success, status_code, error = add_to_cart(domain, variant_id, high, current_proxy)
            if success:
                low = high
                high *= 2
                if high > max_limit:
                    high = max_limit
            else:
                if status_code == 422:
                    high = low + (high - low) // 2  # Adjust high downward more conservatively
                    break  # Stop further adjustments as we've found the upper limit
                else:
                    break  # Break on other errors

        # More sensitive binary search between low and high
        while low < high:
            mid = (low + high + 1) // 2
            success, status_code, error = add_to_cart(domain, variant_id, mid, current_proxy)
            if success:
                low = mid  # Move low up to mid
            else:
                if status_code == 422:
                    high = mid - 1  # Adjust high to just below mid

                else:
                    high = mid - 1  # Adjust high to be more precise by setting it to mid - 1

        results[variant_id] = low  # This should now hold the maximum possible quantity

      

    return results