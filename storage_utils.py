from datetime import datetime
import os

def get_date_subpath():
    """
    Returns a S3/FTP-friendly path based on today's date.
    """
    return datetime.now().strftime('%Y/%m/%d')
