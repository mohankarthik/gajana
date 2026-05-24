import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from plugins.gmail_fetcher.fetcher import run_plugin
from src.google_data_source import GoogleDataSource

def main():
    print("Starting Gajana Gmail Fetcher...")
    ds = GoogleDataSource()
    # Run the plugin, checking the last 30 days of emails
    run_plugin(ds.drive_service, days_back=30)
    print("Finished checking emails.")

if __name__ == "__main__":
    main()
