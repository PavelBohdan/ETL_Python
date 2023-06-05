import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import os


try:
    conn = mysql.connector.connect(read_default_file='./.my.cnf')
    print('Connection successful')

    cur_path = os.getcwd()
    file = 'movies.csv'
    file_path = os.path.join(cur_path, 'data_files', file)

    query = """SELECT year, title, genre, avg_vote, CASE 
                WHEN avg_vote < 3 THEN 'bad' WHEN  avg_vote < 6 THEN 'okay' WHEN avg_vote >= 6 THEN 'good' END 
                AS movie_rating FROM oscarval_sql_course.imdb_movies WHERE year BETWEEN 2005 and 2010 ORDER BY avg_vote"""
    df = pd.read_sql(query, conn)

    yr_2005 = df["year"] == 2005

    df.to_csv(file_path, index=False)

    conn.close()
    print('Connection closed')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check your credentials')
    else:
        print('err')
