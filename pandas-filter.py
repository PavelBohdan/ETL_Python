import mysql.connector
from mysql.connector import errorcode
import pandas as pd


try:
    conn = mysql.connector.connect(read_default_file='./.my.cnf')
    print('Connection successful')

    query = """SELECT year, title, genre, avg_vote FROM oscarval_sql_course.imdb_movies WHERE year BETWEEN 2005 and 2006"""

    df = pd.read_sql(query, conn)

    yr_2005 = df["year"] == 2005
    print(df[~yr_2005].head())

    conn.close()
    print('Connection closed')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check your credentials')
    else:
        print('err')
