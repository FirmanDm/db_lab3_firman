create table genre (
    genre_id int NOT NULL,
    genre_name varchar(150) NOT NULL,
    genre_movies_number int NOT NULL,
    genre_market_share numeric(5, 2) NOT NULL
);

create table distributor (
    dist_id int NOT NULL,
    dist_name varchar(150) NOT NULL,
    dist_movies_number int NOT NULL,
    dist_market_share numeric(5, 2) NOT NULL
);

create table movie (
    movie_id int NOT NULL,
    movie_name varchar(150) NOT NULL,
    movie_year int NOT NULL,
    movie_revenue int NOT NULL,
    dist_id int NOT NULL,
    genre_id int NOT NULL
);

alter table genre add constraint PK_genre PRIMARY KEY (genre_id);
alter table distributor add constraint PK_distributor PRIMARY KEY (dist_id);
alter table movie add constraint PK_movie PRIMARY KEY (movie_id);

alter table movie add constraint FK_movie_genre FOREIGN KEY (genre_id) REFERENCES genre (genre_id);
alter table movie add constraint FK_movie_distributor FOREIGN KEY (dist_id) REFERENCES distributor (dist_id);
