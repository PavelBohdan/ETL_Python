from google.cloud import bigquery

client = bigquery.Client(project='etl-python-388514')

sql = """ SELECT * FROM sample_dataset.movies LIMIT 10"""

query_job = client.query(sql)

results = query_job.result()

for r in results:
    print(r.year, r.title, r.genre, r.avg_vote)
