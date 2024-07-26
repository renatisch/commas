import hmac, hashlib


def generate_signature(endpoint: str, secret: str) -> str:
    signature = hmac.new(
        bytes(secret, "latin-1"),
        msg=bytes(endpoint, "latin-1"),
        digestmod=hashlib.sha256,
    ).hexdigest()
    return signature
