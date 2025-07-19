# Code for ETL operations on Country-GDP data
# Importing the required libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3


#Variable Declaration
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
output_CSV_path = './Largest_banks_data.csv'
db_table_name = 'Largest_banks'
db_name = 'Banks.db'
log_file = 'code_log.txt'
exchange_rates = 'exchange_rate.csv'

def log_info(message):
    ''' This function logs the messages to a log file with a timestamp.
    The log file is created if it does not exist.
    '''
    file = 'code_log.txt'
    timeStamp_format = '%Y-%m-%d %H:%M:%S'
    timeStamp = datetime.now().strftime(timeStamp_format)
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
    df['MC_EUR_Billion'] = df['MC_USD_Billion'] * ex_df.loc['EUR','Rate']
    df['MC_GBP_Billion'] = df['MC_USD_Billion'] * ex_df.loc['GBP','Rate']
    df['MC_INR_Billion'] = df['MC_USD_Billion'] * ex_df.loc['INR','Rate']
    log_info('Transformation: New currency added for EUR, GBP and INR')
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the  provided path. Function returns nothing.'''
    df.to_csv(output_path, index = False)
    log_info('CSV file created: DataFramewritten in CSV file')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    try:
        df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)
    except:
        print("Error encountered while accessing DB")
    finally:
        log_info('CSV file created: DataFramewritten in CSV file')


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    db_df = pd.read_sql(query_statement,sql_connection)
    print(db_df)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
# Extraction
df = extract(url,['Name', 'MC_USD_Billion'])
# Transformation
new_df = transform(df, exchange_rates)
new_df_rounded = new_df.round(2)
# Loading to CSV
load_to_csv(new_df_rounded,output_CSV_path)
# Loading to SQL
log_info('Database: Connection open...')
sql_connection = sqlite3.connect(db_name)
load_to_db(new_df_rounded, sql_connection, db_table_name)
# Querying
# sql_connection = sqlite3.connect(db_name)
query_1 = f'Select * FROM {db_table_name}'
query_2 = f'SELECT AVG(MC_GBP_Billion) FROM {db_table_name}'
query_3 = f'SELECT Name from {db_table_name} LIMIT 5'
run_query(query_1, sql_connection)
# run_query('SELECT Name from Largest_banks LIMIT 5', sql_connection)
sql_connection.close()
log_info('Database: Connection closed!')
