#!/bin/bash

# Activate virtual environment
source ../venv/bin/activate

# Run scraper.py
python3.10 scraper.py
scraper_exit=$?
# If exit status is different than 0, exit script
if [ $scraper_exit -ne 0 ]; then
    exit 1
fi

# Run store.py
python3.10 store.py
store_exit=$?

# Run processor.py
python3.10 processor.py
processor_exit=$?

# Print exit statuses
echo "scraper.py exit status: $scraper_exit"
echo "store.py exit status: $store_exit"
echo "processor.py exit status: $processor_exit"

# MySQL Settings
# User: investment_admin
# Password: -Gs@IVxBY4jLZ$wM
# Database: investment_analysis