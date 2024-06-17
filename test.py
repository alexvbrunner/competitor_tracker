import time
from UTILS.shopify_utils import add_to_cart
import unittest
from itertools import cycle

# Dictionary to track requests per domain and their timestamps
request_count = {}


def check_and_limit_requests(domain, max_requests_per_minute=60):
    current_time = time.time()
    window_start = current_time - 60  # 60 seconds window

    if domain not in request_count:
        request_count[domain] = []

    # Filter out requests outside the current window
    request_count[domain] = [t for t in request_count[domain] if t > window_start]

    if len(request_count[domain]) >= max_requests_per_minute:
        time_to_wait = 60 - (current_time - min(request_count[domain]))
        print(f"Rate limit exceeded for {domain}. Waiting for {time_to_wait} seconds.")
        time.sleep(time_to_wait)
        request_count[domain].clear()  # Reset after waiting

    request_count[domain].append(current_time)


def load_proxies():
    """Load and format proxies with credentials for rotating proxies."""
    username = "qfkdohpl-rotate"
    password = "rdohpw8rodyc"
    domain = "p.webshare.io"
    port = 80
    formatted_proxy = f"http://{username}:{password}@{domain}:{port}"
    return [formatted_proxy]


class TestAddToCart(unittest.TestCase):
    def test_rate_limit_handling(self):
        shopify_store_url = "https://www.miss-sophie.com/"
        variant_id = "45004539265290"
        quantity = 1

        proxies = load_proxies()
        proxy_cycle = cycle(proxies)  # Use itertools.cycle for rotating proxies

        # Run ten tests with a 10-minute wait between each
        for _ in range(10):
            self.simulate_requests(shopify_store_url, variant_id, quantity, proxy_cycle)
            time.sleep(300)  # Wait for 10 minutes before the next test

    def simulate_requests(self, shopify_store_url, variant_id, quantity, proxy_cycle):
        request_count = 0
        max_requests_before_limit = 0

        # Simulate rapid successive requests
        for _ in range(1500):
            current_proxy = next(proxy_cycle)  # Get the next proxy from the cycle
            success, status_code, _ = add_to_cart(shopify_store_url, variant_id, quantity, current_proxy)
            if success:
                request_count += 1
            if not success and status_code == 429:
                max_requests_before_limit = request_count
                print(f"Rate limit handling test passed: 429 detected after {max_requests_before_limit} requests.")
                break
        else:
            print("Rate limit handling test failed: 429 not handled as expected.")

            return max_requests_before_limit
if __name__ == '__main__':
    unittest.main()



    from UTILS.job_module import job  # Ensure job_module.py is properly set up and accessible

def run_job():
    """
    Function to be called to execute the job.
    """
    job()

if __name__ =='__main__':
    run_job()