

def load_proxies():
    """Load and format proxies with credentials for rotating proxies."""
    username = "qfkdohpl-rotate"
    password = "rdohpw8rodyc"
    domain = "p.webshare.io"
    port = 80
    formatted_proxy = f"http://{username}:{password}@{domain}:{port}"
    return [formatted_proxy]
