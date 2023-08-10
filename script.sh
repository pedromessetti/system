#!/bin/bash

# Activate virtual environment
source ../venv/bin/activate

# Run scraper.py
#python3.10 scraper.py

# Run store.py
python3.10 store.py

# Run processor.py
python3.10 processor.py

# MySQL Settings
# User: investment_admin
# Password: -Gs@IVxBY4jLZ$wM
# Database: investment_analysis