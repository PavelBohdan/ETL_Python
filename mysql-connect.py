import mysql.connector
from mysql.connector import errorcode


try:
    conn = mysql.connector.connect(read_default_file='./.my.cnf')
    print('Connection successful')
    conn.close()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check your credentials')
    else:
        print('err')
