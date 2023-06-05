import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import os


try:
    conn = mysql.connector.connect(read_default_file='./.my.cnf')
    print('Connection successful')

    cur_path = os.getcwd()
    file = 'city_housing.csv'
    file_path = os.path.join(cur_path, 'data_files', file)

    query = """SELECT * FROM oscarval_sql_course.city_house_prices"""

    df = pd.read_sql(query, conn)

    # data transformation steps
    df.set_index('Date', inplace=True)
    df = df.stack().reset_index()

    df.columns = ['date', 'city', 'price']

    df.to_csv(file_path, index=False)
    conn.close()
    print('Connection closed')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check your credentials')
    else:
        print('err')
