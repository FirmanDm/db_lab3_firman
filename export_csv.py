import psycopg2.extras
import psycopg2
import csv

OUTPUT_FILE_T = 'lab3_db_{}.csv'

TABLES = [
    'movie',
    'genre',
    'distributor',
]

conn = psycopg2.connect(
    dbname='lab2', user='firman',
    password='777', host='localhost'
)

with conn:
    cur = conn.cursor()

    for table_name in TABLES:
        cur.execute('SELECT * FROM ' + table_name)
        fields = [x[0] for x in cur.description]
        with open(OUTPUT_FILE_T.format(table_name), 'w') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(fields)
            for row in cur:
                writer.writerow([str(x) for x in row])
