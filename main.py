import psycopg2
import psycopg2.extras
import matplotlib.pyplot as plt


def years_best_revenue(cur):
    cursor.execute('drop view if exists YearBestRevenue')
    cursor.execute(
        """
        create view YearBestRevenue as
        select movie_year, movie_revenue from movie;
        """
    )
    cur.execute('select * from YearBestRevenue;')

    years = []
    revenue = []
    for result in cur.fetchall():
        years.append(int(result['movie_year']))
        revenue.append(float(result['movie_revenue']))

    plt.plot(years, revenue)
    plt.ticklabel_format(style='plain')
    plt.show()

def genre_by_market_share(cur):
    cursor.execute('drop view if exists GenreByMarketShare')
    cursor.execute(
        """
        create view GenreByMarketShare as
        select genre_name, genre_market_share from genre;
        """
    )
    cur.execute('select * from GenreByMarketShare;')
    genres = []
    genre_ms = []
    for result in cur.fetchall():
        genres.append(result['genre_name'])
        genre_ms.append(float(result['genre_market_share']))
    patches, texts = plt.pie(genre_ms, startangle=90)
    plt.legend(patches, genres, loc="best")
    # Set aspect ratio to be equal so that pie is drawn as a circle.
    plt.axis('equal')
    plt.tight_layout()
    plt.show()


def distributors_by_movies_num(cur):
    cursor.execute('drop view if exists DistributorByMoviesNumber')
    cursor.execute(
        """
        create view DistributorByMoviesNumber as
        select dist_id, dist_movies_number from distributor;
        """
    )
    cur.execute('select * from DistributorByMoviesNumber;')

    dist = []
    dist_movies_count = []
    for result in cur.fetchall():
        dist.append(int(result['dist_id']))
        dist_movies_count.append(int(result['dist_movies_number']))

    plt.bar(dist, dist_movies_count)
    plt.xticks(dist)
    plt.show()


if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname='lab2', user='firman',
        password='777', host='localhost'
    )
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        years_best_revenue(cursor)
        genre_by_market_share(cursor)
        distributors_by_movies_num(cursor)

    conn.close()
