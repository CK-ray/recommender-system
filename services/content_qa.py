import random
import numpy as np
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity

# 创建数据库连接
def get_db_connection():
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="12345678",
        db="recommendation_db",
        autocommit=True
    )

# 使用 SQLAlchemy 引擎读取数据
engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')

def fetch_all_movies():
    query = "SELECT * FROM movies"
    movies = pd.read_sql(query, engine)
    return movies

def process_genre_input(genre_input):
    genre_input = genre_input.strip().lower()
    genre_map = {
        'action': 'Action',
        'adventure': 'Adventure',
        'animation': 'Animation',
        'comedy': 'Comedy',
        'crime': 'Crime',
        'documentary': 'Documentary',
        'drama': 'Drama',
        'family': 'Family',
        'fantasy': 'Fantasy',
        'history': 'History',
        'horror': 'Horror',
        'music': 'Music',
        'mystery': 'Mystery',
        'romance': 'Romance',
        'sci-fi': 'Science Fiction',
        'science fiction': 'Science Fiction',
        'tv movie': 'TV Movie',
        'thriller': 'Thriller',
        'war': 'War',
        'western': 'Western'
    }
    return genre_map.get(genre_input, genre_input.capitalize())

def calculate_similarity(movies):
    genre_columns = movies.select_dtypes(include=[np.number]).columns
    genre_matrix = movies[genre_columns].values
    similarity_matrix = cosine_similarity(genre_matrix)
    return similarity_matrix

def get_recommendations(movie_id, similarity_matrix, movies):
    try:
        movie_index = movies[movies['movie_id'] == movie_id].index[0]
    except IndexError:
        raise ValueError(f"Movie ID {movie_id} not found in the dataset.")

    similarity_scores = list(enumerate(similarity_matrix[movie_index]))
    similarity_scores = sorted(similarity_scores, key=lambda x: x[1], reverse=True)
    similar_movies = [i[0] for i in similarity_scores[1:21]]
    return movies.iloc[similar_movies]

def content_based_recommendation(genre_input):
    genre = process_genre_input(genre_input)
    movies = fetch_all_movies()

    if genre not in movies.columns:
        raise ValueError(f"Genre '{genre}' not found in movie dataset columns.")

    genre_movies = movies[movies[genre] == 1].reset_index(drop=True)  # Reset the index to ensure consistency
    if genre_movies.empty:
        return None

    similarity_matrix = calculate_similarity(genre_movies)

    selected_movie = genre_movies.sample(n=1).iloc[0]
    selected_movie_id = selected_movie['movie_id']

    try:
        recommendations = get_recommendations(selected_movie_id, similarity_matrix, genre_movies)
    except (ValueError, IndexError) as e:
        print(f"Error in get_recommendations: {e}")
        return None

    recommended_movie = recommendations.sample(n=1).iloc[0]
    return fetch_movie_by_id(recommended_movie['movie_id'])

def fetch_movie_by_id(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT movie_id, movie_title, overview, director, cast, release_date
        FROM movies
        WHERE movie_id = %s
    """, (movie_id,))
    movie = cursor.fetchone()
    cursor.close()
    conn.close()
    return movie

def fetch_movie_by_title(title):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT movie_id, movie_title, overview, director, cast, release_date
        FROM movies
        WHERE movie_title = %s
    """, (title,))
    movie = cursor.fetchone()
    cursor.close()
    conn.close()
    return movie

def fetch_avg_rating_by_movie_id(movie_id):
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
    return avg_rating[0] if avg_rating else None


