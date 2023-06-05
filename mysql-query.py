import mysql.connector
from mysql.connector import errorcode


try:
    conn = mysql.connector.connect(read_default_file='./.my.cnf')
    print('Connection successful')
    cursor = conn.cursor()

    query = """SELECT year, title, genre FROM oscarval_sql_course.imdb_movies LIMIT 7"""

    cursor.execute(query)

    for (year, title, genre) in cursor:
        print(year, title, genre)

    conn.close()
    print('Connection closed')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check your credentials')
    else:
        print('err')
