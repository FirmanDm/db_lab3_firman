DO $$
DECLARE
    new_name movie.movie_name%TYPE;
    new_year movie.movie_year%TYPE;
    new_reve movie.movie_revenue%TYPE;

BEGIN
    new_name := 'Test film ';
    new_year := 2018;
    new_reve := 500000000;


    FOR counter IN 1..5
        LOOP
            INSERT INTO movie(movie_id, movie_name, movie_year, movie_revenue, dist_id, genre_id)
            VALUES (24+counter, new_name || counter, new_year + counter, new_reve + 10000000*counter, counter, 6-counter);
        END LOOP;
END
$$