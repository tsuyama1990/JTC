class APIConnectionError(Exception):
    """Exception raised for errors in the J-Quants API connection or rate limits after retries."""
