import time
import logging
import functools
import random
from typing import Callable, Any, Optional

logger = logging.getLogger(__name__)

class RetryError(Exception):
    """Exception raised when all retry attempts have been exhausted."""
    pass

def with_retry(
    max_attempts: int = 3,
    retry_delay: float = 1.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    exceptions_to_retry: tuple = (Exception,),
    should_retry: Optional[Callable[[Exception], bool]] = None
) -> Callable:
    """
    A decorator for retrying functions that may fail due to transient errors.
    
    Args:
        max_attempts: Maximum number of retry attempts.
        retry_delay: Initial delay between retries in seconds.
        backoff_factor: Multiplier applied to delay between retries.
        jitter: Whether to add randomness to the retry delay.
        exceptions_to_retry: Tuple of exception types to retry on.
        should_retry: Optional function that takes an exception and returns
                     whether to retry based on the exception.
    
    Returns:
        The decorated function.
        
    Example:
        @with_retry(max_attempts=3, exceptions_to_retry=(ConnectionError, Timeout))
        def fetch_data():
            # code that might fail
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 1
            last_exception = None
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions_to_retry as e:
                    last_exception = e
                    
                    # Check if we should retry based on the exception
                    if should_retry and not should_retry(e):
                        logger.warning(f"Not retrying {func.__name__} based on exception: {str(e)}")
                        break
                    
                    # Calculate retry delay with exponential backoff
                    delay = retry_delay * (backoff_factor ** (attempt - 1))
                    
                    # Add jitter to avoid thundering herd problem
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    # Check if this is the last attempt
                    if attempt >= max_attempts:
                        logger.warning(f"Final attempt {attempt}/{max_attempts} for {func.__name__} failed: {str(e)}")
                        break
                        
                    logger.warning(f"Attempt {attempt}/{max_attempts} for {func.__name__} failed: {str(e)}. "
                                 f"Retrying in {delay:.2f} seconds...")
                    
                    # Wait before retrying
                    time.sleep(delay)
                    attempt += 1
            
            # If we've exhausted all retries, raise the last exception
            logger.error(f"All {max_attempts} attempts for {func.__name__} failed.")
            if last_exception:
                raise RetryError(f"Function {func.__name__} failed after {max_attempts} attempts") from last_exception
            else:
                raise RetryError(f"Function {func.__name__} failed after {max_attempts} attempts")
                
        return wrapper
    return decorator