from functools import wraps
import time
from datetime import datetime

class RateLimiter:
    def __init__(self, cooldown):
        self.cooldown = cooldown
        self.last_call = None

    def reset(self):
        self.last_call = None

    @staticmethod
    def rate_limited(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.rate_limiter.last_call is not None:
                elapsed = time.time() - self.rate_limiter.last_call
                if elapsed < self.rate_limiter.cooldown:
                    wait_time = self.rate_limiter.cooldown - elapsed
                    print(f"Rate limit reached. Waiting {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
            
            result = func(self, *args, **kwargs)
            self.rate_limiter.last_call = time.time()
            return result
        return wrapper

def format_number(num):
    """Format numbers for display"""
    if num >= 1000000:
        return f"{num/1000000:.1f}M"
    elif num >= 1000:
        return f"{num/1000:.1f}K"
    return str(num)

def calculate_trend(current, previous):
    """Calculate trend percentage"""
    if previous == 0:
        return 100 if current > 0 else 0
    return ((current - previous) / previous) * 100
