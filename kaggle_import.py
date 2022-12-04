import csv
import psycopg2
import psycopg2.extras


query_1 = '''
DELETE FROM movie
'''
query_2 = '''
INSERT INTO movie (movie_id, movie_name, movie_year, movie_revenue, genre_id, dist_id) VALUES (%s, %s, %s, %s, %s, %s)
'''


def import_movies(cur):
    cur.execute(query_1)
    with open("raw data/HighestGrossers.csv") as csv_file:
        movie_id = 0
        for row in csv.DictReader(csv_file):
            movie_id = movie_id + 1
            movie_name = row['MOVIE']
            movie_year = int(row['п»їYEAR'])
            movie_revenue = int(row['TOTAL IN 2019 DOLLARS'].replace('$', '').replace(',', ''))

            genre_id = row['GENRE']
            if not genre_id:
                continue
            cur.execute('select genre_id from genre where genre_name = %s', (genre_id,))
            genre_id = cur.fetchone()[0]

            dist_id = row['DISTRIBUTOR']
            cur.execute('select dist_id from distributor where dist_name = %s', (dist_id,))
            dist_id = cur.fetchone()[0]
            values = [movie_id, movie_name, movie_year, movie_revenue, genre_id, dist_id]
            cur.execute(query_2, values)


query_3 = '''
DELETE FROM genre
'''
query_4 = '''
INSERT INTO genre (genre_id, genre_name, genre_movies_number, genre_market_share) VALUES (%s, %s, %s, %s)
'''


def import_genres(cur):
    cur.execute(query_3)
    with open("raw data/TopGenres.csv") as csv_file:
        genre_id = 0
        for row in csv.DictReader(csv_file):
            genre_id = genre_id + 1
            genre_name = row['GENRES']
            genre_movies_number = row['MOVIES']
            if ',' in genre_movies_number:
                genre_movies_number = genre_movies_number.replace(',', '')
            genre_market_share = float(row['MARKET SHARE'].replace('%', ''))
            values = [genre_id, genre_name, genre_movies_number, genre_market_share]
            cur.execute(query_4, values)


query_5 = '''
DELETE FROM distributor
'''
query_6 = '''
INSERT INTO distributor (dist_id, dist_name, dist_movies_number, dist_market_share) VALUES (%s, %s, %s, %s)
'''


def import_dist(cur):
    cur.execute(query_5)
    with open("raw data/TopDistributors.csv") as csv_file:
        dist_id = 0
        for row in csv.DictReader(csv_file):
            dist_id = dist_id + 1
            dist_name = row['DISTRIBUTORS']
            dist_movies_number = row['MOVIES']
            dist_market_share = float(row['MARKET SHARE'].replace('%', ''))
            values = [dist_id, dist_name, dist_movies_number, dist_market_share]
            cur.execute(query_6, values)


if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname='lab2', user='firman',
        password='777', host='localhost'
    )
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute(query_1)
        import_genres(cursor)
        conn.commit()

        import_dist(cursor)
        conn.commit()

        import_movies(cursor)
        conn.commit()
    conn.close()
