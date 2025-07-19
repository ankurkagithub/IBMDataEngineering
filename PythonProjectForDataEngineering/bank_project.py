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
    ''' This function logs the messages to a log file with a timestamp.
    The log file is created if it does not exist.
    '''
    file = 'code_log.txt'
    timeStamp_format = '%Y-%m-%d %H:%M:%S'
    timeStamp = datetime.now().strftime(timestamp_format)
    with open(file, 'a+') as f:
        f.write(f'{timeStamp} - {message}\n')

def extract(url, table_attribs):
    '''
    Scraping data from provided URL, parsing it and converting it into
    a DataFrame.
    '''
    response = requests.get(url)
    log_info(f'URL Fetched: status code is {response.status_code}')
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find_all('table')[0]
        rows = table.find_all('tr')
        rows = rows[1:]
        data = {}
        # data['rank'] = []
        data[table_attribs[0]] = []
        data[table_attribs[1]] = []
        for row in rows:
            # rank = int(row.find_all('td')[0].text.strip())
            bank_name = row.find_all('td')[1].find_all('a')[-1].text
            market_cap = float(row.find_all('td')[2].text.strip())
            # data['rank'].append(rank)
            data[table_attribs[0]].append(bank_name)
            data[table_attribs[1]].append(market_cap)
        df = pd.DataFrame(data)
        log_info('Extraction: data extracted and loaded into a DataFrame')
        return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies
    '''
    ex_df = pd.read_csv(exchange_rates, index_col='Currency') # Converting currency into a DF
    df['market_cap(EUR_billion)'] = df['market_cap(USD_billion)'] * ex_df.loc['EUR','Rate']
    df['market_cap(GBP_billion)'] = df['market_cap(USD_billion)'] * ex_df.loc['GBP','Rate']
    df['market_cap(INR_billion)'] = df['market_cap(USD_billion)'] * ex_df.loc['INR','Rate']
    log_info('Transformation: New currency added for EUR, GBP and INR')
    return df
