import json
import psycopg2

conn = psycopg2.connect(
    dbname='lab2', user='firman',
    password='777', host='localhost',
    port='5432'
)
data = {}
with conn:
    cur = conn.cursor()

    for table in ('movie', 'genre', 'distributor'):
        cur.execute('SELECT * FROM ' + table)
        rows = []
        fields = [x[0] for x in cur.description]

    for row in cur:
        rows.append(dict(zip(fields, row)))
    data[table] = rows

    with open('all_data.json', 'w') as outf:
        json.dump(data, outf, default=str)
