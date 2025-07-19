#imports
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3


#variables declations
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
output_CSV_path = './Largest_banks_data.csv
db_name = 'Banks.db'
db_table_name = 'Largest_banks'
log_file = 'code_log.txt'


def log_info(message):
    file = 'code_log.txt'
    timeStamp_format = '%Y-%m-%d %H:%M:%S'
    timeStamp = datetime.now().strftime(timestamp_format)
    with open(file, 'a+') as f:
        f.write(f'{timeStamp} - {message}\n')
