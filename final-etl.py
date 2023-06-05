# modules
import os
import mysql.connector
import pandas as pd
from google.cloud import bigquery

# variables
cur_path = os.getcwd()
load_file = 'mysql_export.csv'
load_file = os.path.join(cur_path, 'data_files', load_file)


proj = 'etl-python-388514'
dataset = 'sample_dataset'
target_table = 'annual_movie_summary'
table_id = f'{proj}.{dataset}.{target_table}'

# data connections
conn = mysql.connector.connect(read_default_file='./.my.cnf')
client = bigquery.Client(project=proj)

# create SQL extract query
sql = """ SELECT year, 
                count(imdb_title_id) as movie_count, 
                avg(duration) as avg_movie_duration, 
                avg(avg_vote) as avg_rating 
                FROM oscarval_sql_course.imdb_movies GROUP BY 1"""

# extract data
df = pd.read_sql(sql, conn)

# transform data


def year_rating(r):
    if r <= 5.65:
        return 'ban movie year'
    elif r <= 5.9:
        return 'okay movie year'
    elif r <= 10:
        return 'good movie year'
    else:
        return 'no rated'


df['year_rating'] = df['avg_rating'].apply(year_rating)
df.to_csv(load_file, index=False)

# load data
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)

# open file for loading
with open(load_file, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file, table_id, job_config=job_config)

load_job.result()

# check how many records where loaded
dest_table = client.get_table(table_id)
print(f'You have {dest_table.num_rows} in your table')
