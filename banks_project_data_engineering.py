# Code for ETL operations on Country-GDP data

# Importing the required libraries

import datetime
import requests
import sqlite3
import pandas as pd
import numpy as np 
from bs4 import BeautifulSoup


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    with open("code_log.txt", "a") as log_file:
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{time_stamp}: {message}\n"
        log_file.write(log_entry)

# Dictionary to map function names to log messages
log_messages = {
    "Declaring known values": "Preliminaries complete. Initiating ETL process",
    "Call extract() function": "Data extraction complete. Initiating Transformation process",
    "Call transform() function ": "Data transformation complete. Initiating Loading process",
    "Call load_to_csv()": "Data saved to CSV file",
    "Initiate SQLite3 connection": "SQL Connection initiated",
    "Call load_to_db()": "Data loaded to Database as a table, Executing queries",
    "Call run_query()": "Process Complete",
    "Close SQLite3 connection": "Server Connection closed"
}

def extract(url, table_attribs=None):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    # Fetch the HTML content of the page
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    # Scraping the required information
    if table_attribs:
        tables = data.find_all('table', attrs=table_attribs)
    else:
        tables = data.find_all('table')

    rows = tables[0].find('tbody').find_all('tr')

    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=["Rank", "Bank_name", "Market_Cap(USD)"])

    # Iterate over the rows to find the required data
    count = 0
    for row in rows:
        if count < 10:
            col = row.find_all('td')
            if len(col) != 0:
                data_dict = {
                    "Rank": col[0].get_text(strip=True),
                    "Bank_name": col[1].get_text(strip=True),
                    "Market_Cap(USD)": col[2].get_text(strip=True)
                }
                df = pd.concat([df, pd.DataFrame([data_dict])], ignore_index=True)
                count += 1
        else:
            break
    log_progress(log_messages["Call extract() function"])
    return df

url='https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
df=extract(url)

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rates_df = pd.read_csv(csv_path)
    exchange_rates = exchange_rates_df.set_index('Currency')['Rate'].to_dict()
    df['MC_GBP_Billion'] = df['Market_Cap(USD)'].apply(lambda x: np.round(float(x.replace(',', '')) * exchange_rates['GBP'], 2))
    df['MC_EUR_Billion'] = df['Market_Cap(USD)'].apply(lambda x: np.round(float(x.replace(',', '')) * exchange_rates['EUR'], 2))
    df['MC_INR_Billion'] = df['Market_Cap(USD)'].apply(lambda x: np.round(float(x.replace(',', '')) * exchange_rates['INR'], 2))
    log_progress(log_messages["Call transform() function "])
    return df

# call the transform() function
csv_path = "/home/project/exchange_rate.csv"
df_transformed = transform(df, csv_path)


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    log_progress(log_messages["Call load_to_csv()"])
    df.to_csv(output_path, index=False)

#load the csv file  on the output path 
output_path = "/home/project/banks_project.csv"
load_to_csv(df, output_path)



def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    log_progress(log_messages["Call load_to_db()"])
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Define the database file name and table name
database_path = "/home/project/Banks.db" 
table_name = "Largest_banks"

# Establish a connection to the SQLite database
sql_connection = sqlite3.connect(database_path)

# Call the load_to_db function
load_to_db(df_transformed, sql_connection, table_name)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    # Create a cursor object to communicate with the dabase 
    cursor=sql_connection.cursor()
    # Print the query statement
    print(f"Executing query: {query_statement}\n")
    # Execute the query
    cursor.execute(query_statement)
    # Fetch the results
    results = cursor.fetchall()
    # Print the results
    for row in results:
        print(row)
    # Close the cursor
    cursor.close()


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Establish the SQLite3 connection
sql_connection = sqlite3.connect('Banks.db')

# define the query
query_statement={
'query_statement1' : 'SELECT * FROM Largest_banks',
'query_statement2' : 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
'query_statement3' : 'SELECT Bank_name from Largest_banks LIMIT 5'
}
for key, query in  query_statement.items():
    print('*'*10)
    run_query(query,sql_connection)
    
log_progress(log_messages["Close SQLite3 connection"])
sql_connection.close()

