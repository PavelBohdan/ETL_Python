import mysql.connector
from mysql.connector import errorcode
import pandas as pd


try:
    conn = mysql.connector.connect(read_default_file='./.my.cnf')
    print('Connection successful')

    query = """SELECT year, title, genre FROM oscarval_sql_course.imdb_movies LIMIT 7"""

    df = pd.read_sql(query, conn)
    print(df)
    print(df.dtypes())

    conn.close()
    print('Connection closed')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check your credentials')
    else:
        print('err')
