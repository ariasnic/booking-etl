from functools import wraps
from datetime import datetime, timezone
import logging
    

def logger(fn):

    @wraps(fn)
    def inner(*args, **kwargs):
        called_at = datetime.now(timezone.utc)
        logging.info(f">>> Running {fn.__name__!r} function. Logged at {called_at}")
        to_execute = fn(*args, **kwargs)
        logging.info(f">>> Function: {fn.__name__!r} executed. Logged at {called_at}")
        return to_execute

    return inner
