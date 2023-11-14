import psycopg2
import pandas as pd 
import config 

# PART 1: Create Database
def gloabl_create_database():
    try:
        conn = psycopg2.connect(f"host={config.HOST} dbname={config.GLOBAL_DBNAME} user={config.USERNAME} password={config.PASSWORD}")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        cur.execute(f"DROP DATABASE {dbname}")
        print('Drop Statement Executed!')
        cur.execute(f"CREATE DATABASE {dbname}")
        print('Create Statement Executed!')

        conn.close()
        
        print('Create Database Function Executed!')
    except psycopg2.Error as e:
        print(e)
        return None, None

def create_connection(dbname):
    try:
        conn = psycopg2.connect(f"host={config.HOST} dbname={dbname} user={config.USERNAME} password={config.PASSWORD}")
        cur = conn.cursor()
        print('Create Database Function Executed!')
        return conn, cur
    except psycopg2.Error as e:
        print(e)
        return None, None

gloabl_create_database()
conn, cur = create_connection(config.DBNAME)

# PART 2: Prepare Datasets
def prepare_datasets():
    accounts_country = pd.read_csv('./data/Wealth-AccountsCountry.csv')
    accounts_country_clean = accounts_country[['Code', 'Short Name', 'Table Name', 'Long Name', 'Currency Unit']]
    print(accounts_country_clean.head(10))
    
    accounts_data = pd.read_csv('./data/Wealth-AccountData.csv')
    accounts_data_clean = accounts_data[['Country Name', 'Country Code', 'Series Name', '1995 [YR1995]', '2000 [YR2000]', '2005 [YR2005]', '2010 [YR2010]', '2015 [YR2015]']]
    accounts_data_clean = accounts_data_clean.replace('..', 0)
    print(accounts_data_clean.head(10))

    account_series = pd.read_csv('./data/Wealth-AccountSeries.csv')
    account_series_clean = account_series[['Code', 'Indicator Name', 'Long definition', 'Topic']]
    print(account_series_clean.head(10))

    return accounts_country_clean, accounts_data_clean, account_series_clean
accounts_country, accounts_data, accounts_series = prepare_datasets()

# PART 3: Create Tables
def execute_query(conn, cur, query, ls = None):
    try:
        cur.execute(query, ls)
        conn.commit()
        print('Query Executed!')
    except psycopg2.Error as e:
        conn.rollback()
        print(ls)
        print(e)

accounts_country_create_qry = """
    CREATE TABLE IF NOT EXISTS accountscountry (
        country_code VARCHAR PRIMARY KEY NOT NULL,
        short_name VARCHAR,
        table_name VARCHAR,
        long_name VARCHAR,
        currency_unit VARCHAR
    );
"""
accounts_data_create_qry = """
    CREATE TABLE IF NOT EXISTS accountsdata (
        country_name VARCHAR,
        country_code VARCHAR,
        indicator_name VARCHAR,
        year_1995 NUMERIC,
        year_2000 NUMERIC,
        year_2005 NUMERIC,
        year_2010 NUMERIC,
        year_2015 NUMERIC
    );
"""
accounts_series_create_qry = """
    CREATE TABLE IF NOT EXISTS accountseries (
        series_code VARCHAR,
        tpoic VARCHAR,
        indicator_name VARCHAR,
        long_definition VARCHAR
    );
"""

execute_query(conn, cur, accounts_country_create_qry)
execute_query(conn, cur, accounts_data_create_qry)
execute_query(conn, cur, accounts_series_create_qry)


# PART 4: Insert Into Tables
accounts_country_insert_qry = "INSERT INTO accountscountry (country_code, short_name, table_name, long_name, currency_unit) VALUES (%s, %s, %s, %s, %s);"

accounts_data_insert_qry = "INSERT INTO accountsdata (country_name, country_code, indicator_name, year_1995, year_2000, year_2005, year_2010, year_2015) VALUES (%s, %s, %s, %s, %s, %s, %s, %s); "

accounts_series_insert_qry = "INSERT INTO accountseries (series_code, tpoic, indicator_name, long_definition) VALUES (%s, %s, %s, %s); "

for idx, row in accounts_country.iterrows(): 
    execute_query(conn, cur, accounts_country_insert_qry, list(row))

for idx, row in accounts_data.iterrows(): 
    execute_query(conn, cur, accounts_data_insert_qry,list(row))

for idx, row in accounts_series.iterrows(): 
    execute_query(conn, cur, accounts_series_insert_qry, list(row))
    
conn.close()