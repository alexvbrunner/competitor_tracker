import time


def check_rate_limit(processed_count, start_time):
    if processed_count >= 100:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time < 300:
            time_to_wait = 300 - elapsed_time
            print(f"Rate limit reached. Pausing for {time_to_wait} seconds.")
            time.sleep(time_to_wait)
            start_time = time.time()
            processed_count = 0

