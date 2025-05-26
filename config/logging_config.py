import logging
import os

def setup_logging():
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        filename="logs/data.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    
    # Configure specific loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    
    # Add console handler if needed (useful during development)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def get_logger(name):
    """
    Returns a configured logger with the given name.
    
    Args:
        name (str): Usually __name__ of the calling module
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name)