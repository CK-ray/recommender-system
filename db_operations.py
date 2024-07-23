import pymysql
import bcrypt
from datetime import datetime
def get_db_connection():
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="12345678",
        db="recommendation_db",
        autocommit=True
    )

def fetch_main_carousel_movies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT movie_id, movie_title, poster_url FROM movies ORDER BY RAND() LIMIT 5")
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return movies

def fetch_recommended_movies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT movie_id, movie_title, poster_url FROM movies ORDER BY RAND() LIMIT 6")
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return movies

def fetch_latest_movies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT movie_id, movie_title, poster_url FROM movies ORDER BY release_date DESC LIMIT 6")
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return movies

def fetch_highest_rated_movies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.movie_id, m.movie_title, m.poster_url, AVG(r.rating) as avg_rating
        FROM movies m
        JOIN ratings r ON m.movie_id = r.movie_id
        GROUP BY m.movie_id, m.movie_title, m.poster_url
        ORDER BY avg_rating DESC
        LIMIT 6
    """)
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return movies

def fetch_all_rated_movies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.movie_id, m.movie_title, m.poster_url, AVG(r.rating) as avg_rating
        FROM movies m
        JOIN ratings r ON m.movie_id = r.movie_id
        GROUP BY m.movie_id, m.movie_title, m.poster_url
        ORDER BY avg_rating DESC
    """)
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return movies

def fetch_movie_details(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT movie_id, movie_title, release_date, IMDb_URL, unknown, Action, Adventure, Animation, Comedy, Crime, Documentary, Drama, Family, Fantasy, History, Horror, Music, Mystery, Romance, `Science Fiction`, `TV Movie`, Thriller, War, Western, poster_url, overview, director, cast
        FROM movies
        WHERE movie_id = %s
    """, (movie_id,))
    movie = cursor.fetchone()
    cursor.close()
    conn.close()
    return movie


def fetch_movie_average_rating(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT AVG(rating) as avg_rating
        FROM ratings
        WHERE movie_id = %s
    """, (movie_id,))
    avg_rating = cursor.fetchone()
    cursor.close()
    conn.close()
    return avg_rating

def fetch_movie_trailer(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT trailer_url
        FROM trailers
        WHERE movie_id = %s
    """, (movie_id,))
    trailers = cursor.fetchall()
    cursor.close()
    conn.close()
    return trailers


def fetch_all_categories():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT category
        FROM (
            SELECT 'Action' AS category FROM movies WHERE Action = 1
            UNION SELECT 'Adventure' FROM movies WHERE Adventure = 1
            UNION SELECT 'Animation' FROM movies WHERE Animation = 1
            UNION SELECT 'Comedy' FROM movies WHERE Comedy = 1
            UNION SELECT 'Crime' FROM movies WHERE Crime = 1
            UNION SELECT 'Documentary' FROM movies WHERE Documentary = 1
            UNION SELECT 'Drama' FROM movies WHERE Drama = 1
            UNION SELECT 'Family' FROM movies WHERE Family = 1
            UNION SELECT 'Fantasy' FROM movies WHERE Fantasy = 1
            UNION SELECT 'History' FROM movies WHERE History = 1
            UNION SELECT 'Horror' FROM movies WHERE Horror = 1
            UNION SELECT 'Music' FROM movies WHERE Music = 1
            UNION SELECT 'Mystery' FROM movies WHERE Mystery = 1
            UNION SELECT 'Romance' FROM movies WHERE Romance = 1
            UNION SELECT 'Science Fiction' FROM movies WHERE `Science Fiction` = 1
            UNION SELECT 'TV Movie' FROM movies WHERE `TV Movie` = 1
            UNION SELECT 'Thriller' FROM movies WHERE Thriller = 1
            UNION SELECT 'War' FROM movies WHERE War = 1
            UNION SELECT 'Western' FROM movies WHERE Western = 1
        ) AS categories
    """)
    categories = cursor.fetchall()
    cursor.close()
    conn.close()
    return [category[0] for category in categories]


def fetch_movies_by_category(category):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = f"""
            SELECT 
                m.movie_id, 
                m.movie_title, 
                m.release_date, 
                m.IMDb_URL, 
                m.poster_url, 
                m.overview, 
                m.director, 
                m.cast,
                IFNULL(AVG(r.rating), 0) as avg_rating
            FROM movies m
            LEFT JOIN ratings r ON m.movie_id = r.movie_id
            WHERE m.`{category}` = 1
            GROUP BY m.movie_id, m.movie_title, m.release_date, m.IMDb_URL, m.poster_url, m.overview, m.director, m.cast
        """
    cursor.execute(query)
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return movies

def check_user_credentials(username, password):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT user_id, username, email, preferred_genres
        FROM users
        WHERE username = %s AND password = %s
    """, (username, password))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return user


def insert_new_user(username, password, email):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (username, password, email)
        VALUES (%s, %s, %s)
    """, (username, password, email))
    conn.commit()
    cursor.close()
    conn.close()


def update_user_preferred_genres(user_id, preferred_genres):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE users
        SET preferred_genres = %s
        WHERE user_id = %s
    """, (preferred_genres, user_id))
    conn.commit()
    cursor.close()
    conn.close()


def get_user_info(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT user_id, username, password, email, preferred_genres
        FROM users
        WHERE user_id = %s
    """, (user_id,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return user


def update_user_info(user_id, username, password, email, preferred_genres):
    conn = get_db_connection()
    cursor = conn.cursor()

    if password:  # 如果有新密码，哈希化
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        cursor.execute("""
            UPDATE users
            SET username = %s, password = %s, email = %s, preferred_genres = %s
            WHERE user_id = %s
        """, (username, hashed_password, email, preferred_genres, user_id))
    else:  # 如果没有新密码，不更新密码
        cursor.execute("""
            UPDATE users
            SET username = %s, email = %s, preferred_genres = %s
            WHERE user_id = %s
        """, (username, email, preferred_genres, user_id))

    conn.commit()
    cursor.close()
    conn.close()

def get_favorite_movies(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT favorite_movies
        FROM users
        WHERE user_id = %s
    """, (user_id,))
    favorite_movies = cursor.fetchone()
    cursor.close()
    conn.close()
    return favorite_movies

def update_favorite_movies(user_id, favorite_movies):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE users
        SET favorite_movies = %s
        WHERE user_id = %s
    """, (favorite_movies, user_id))
    conn.commit()
    cursor.close()
    conn.close()


def remove_favorite_movie(user_id, movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    # 获取当前收藏的电影列表
    cursor.execute("""
        SELECT favorite_movies
        FROM users
        WHERE user_id = %s
    """, (user_id,))
    favorite_movies = cursor.fetchone()[0]

    if favorite_movies:
        movie_list = favorite_movies.split(',')
        if str(movie_id) in movie_list:
            movie_list.remove(str(movie_id))
            new_favorite_movies = ','.join(movie_list)

            # 更新用户的收藏电影列表
            cursor.execute("""
                UPDATE users
                SET favorite_movies = %s
                WHERE user_id = %s
            """, (new_favorite_movies, user_id))
            conn.commit()

    cursor.close()
    conn.close()

def search_movies(query):
    conn = get_db_connection()
    cursor = conn.cursor()
    search_query = f"%{query}%"
    cursor.execute("""
        SELECT movie_id, movie_title, release_date, IMDb_URL, poster_url
        FROM movies
        WHERE movie_title LIKE %s
        ORDER BY movie_title
    """, (search_query,))
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return movies


def get_ratings_count():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT movie_id, COUNT(*) as ratings_count
        FROM ratings
        GROUP BY movie_id
    """)
    ratings_count = cursor.fetchall()
    cursor.close()
    conn.close()
    return ratings_count



def add_movie_rating(user_id, movie_id, rating):
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("""
        INSERT INTO ratings (user_id, movie_id, rating, timestamp)
        VALUES (%s, %s, %s, %s)
    """, (user_id, movie_id, rating, timestamp))
    conn.commit()
    cursor.close()
    conn.close()

