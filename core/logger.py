import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger():
    # Ensure logs directory exists securely
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    log_file = os.path.join(log_dir, "quant_system.log")
    
    # Construct master engine logger
    logger = logging.getLogger("QuantEngine")
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers inside reactive architectures like Streamlit
    if not logger.handlers:
        # Create rotating file handler (5MB max size, preserve 3 archival cascades)
        file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
        file_handler.setLevel(logging.INFO)
        
        # Premium format styling
        formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | [%(name)s] %(message)s')
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        
    return logger

# Global static trace
logger = setup_logger()
