import logging

from flask import jsonify, request
import db_operations
import bcrypt
from decimal import Decimal
import json

def get_main_carousel_movies():
    movies = db_operations.fetch_main_carousel_movies()
    movie_list = []
    for movie in movies:
        movie_dict = {
            "movie_id": movie[0],
            "movie_title": movie[1],
            "poster_url": movie[2]
        }
        movie_list.append(movie_dict)
    return jsonify(movie_list)

def get_recommended_movies():
    movies = db_operations.fetch_recommended_movies()
    movie_list = []
    for movie in movies:
        movie_dict = {
            "movie_id": movie[0],
            "movie_title": movie[1],
            "poster_url": movie[2]
        }
        movie_list.append(movie_dict)
    return jsonify(movie_list)

def get_latest_movies():
    movies = db_operations.fetch_latest_movies()
    movie_list = []
    for movie in movies:
        movie_dict = {
            "movie_id": movie[0],
            "movie_title": movie[1],
            "poster_url": movie[2]
        }
        movie_list.append(movie_dict)
    return jsonify(movie_list)

def get_highest_rated_movies():
    movies = db_operations.fetch_highest_rated_movies()
    movie_list = []
    for movie in movies:
        movie_dict = {
            "movie_id": movie[0],
            "movie_title": movie[1],
            "poster_url": movie[2],
            "avg_rating": float(movie[3])
        }
        movie_list.append(movie_dict)
    return jsonify(movie_list)

def get_all_rated_movies():
    movies = db_operations.fetch_all_rated_movies()
    movie_list = []
    for movie in movies:
        movie_dict = {
            "movie_id": movie[0],
            "movie_title": movie[1],
            "poster_url": movie[2],
            "avg_rating": float(movie[3])
        }
        movie_list.append(movie_dict)
    return jsonify(movie_list)

def get_movie_details(movie_id):
    movie = db_operations.fetch_movie_details(movie_id)
    if movie is None:
        return jsonify({"error": "Movie not found"}), 404

    movie_dict = {
        "movie_id": movie[0],
        "movie_title": movie[1],
        "release_date": movie[2],
        "IMDb_URL": movie[3],
        "unknown": movie[4],
        "Action": movie[5],
        "Adventure": movie[6],
        "Animation": movie[7],
        "Comedy": movie[8],
        "Crime": movie[9],
        "Documentary": movie[10],
        "Drama": movie[11],
        "Family": movie[12],
        "Fantasy": movie[13],
        "History": movie[14],
        "Horror": movie[15],
        "Music": movie[16],
        "Mystery": movie[17],
        "Romance": movie[18],
        "Science_Fiction": movie[19],
        "TV_Movie": movie[20],
        "Thriller": movie[21],
        "War": movie[22],
        "Western": movie[23],
        "poster_url": movie[24],
        "overview": movie[25],
        "director": movie[26],
        "cast": movie[27]
    }
    return jsonify(movie_dict)

def get_movie_average_rating(movie_id):
    avg_rating = db_operations.fetch_movie_average_rating(movie_id)
    if avg_rating is None or avg_rating[0] is None:
        return jsonify({"movie_id": movie_id, "avg_rating": None}), 404

    return jsonify({"movie_id": movie_id, "avg_rating": float(avg_rating[0])})

def get_movie_trailer(movie_id):
    trailers = db_operations.fetch_movie_trailer(movie_id)
    if not trailers:
        return jsonify({"error": "Trailer not found"}), 404

    trailer_list = [trailer[0] for trailer in trailers]
    return jsonify({"movie_id": movie_id, "trailers": trailer_list})

def get_all_categories():
    categories = db_operations.fetch_all_categories()
    return jsonify(categories)

def get_movies_by_category(category):
    category = category.replace('_', ' ')
    movies = db_operations.fetch_movies_by_category(category)
    movie_list = []
    for movie in movies:
        movie_dict = {
            "movie_id": movie[0],
            "movie_title": movie[1],
            "release_date": movie[2],
            "IMDb_URL": movie[3],
            "poster_url": movie[4],
            "overview": movie[5],
            "director": movie[6],
            "cast": movie[7],
            "avg_rating": float(movie[8])  # 将平均评分转换为浮点数
        }
        movie_list.append(movie_dict)
    return jsonify(movie_list)

def login_user():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    user = db_operations.check_user_credentials(username, password)
    if user:
        user_dict = {
            "user_id": user[0],
            "username": user[1],
            "email": user[2],
            "preferred_genres": user[3]
        }
        return jsonify(user_dict)
    else:
        return jsonify({"error": "Invalid username or password"}), 401



def register_user():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')

    # 先检查是否存在相同的用户名
    existing_user = db_operations.check_user_credentials(username, password)
    if existing_user:
        return jsonify({"error": "Username already exists"}), 409

    # 哈希密码
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    db_operations.insert_new_user(username, hashed_password.decode('utf-8'), email)
    return jsonify({"message": "User registered successfully"})


def update_user_genres(current_user):
    data = request.json
    preferred_genres = data.get('preferred_genres')

    if not preferred_genres:
        return jsonify({"error": "Invalid input"}), 400

    db_operations.update_user_preferred_genres(current_user, preferred_genres)
    return jsonify({"message": "User preferred genres updated successfully"})

def get_user_info(current_user):
    user = db_operations.get_user_info(current_user)
    if user:
        user_dict = {
            "user_id": user[0],
            "username": user[1],
            "password": user[2],
            "email": user[3],
            "preferred_genres": user[4]
        }
        return jsonify(user_dict)
    else:
        return jsonify({"error": "User not found"}), 404

def update_user_info(current_user):
    data = request.json
    username = data.get('username')
    password = data.get('password')  # 新密码（如果有的话）
    email = data.get('email')
    preferred_genres = data.get('preferred_genres')

    if not username or not email or not preferred_genres:
        return jsonify({"error": "Invalid input"}), 400

    db_operations.update_user_info(current_user, username, password, email, preferred_genres)
    return jsonify({"message": "User information updated successfully"})

def get_favorite_movies(current_user):
    favorite_movies = db_operations.get_favorite_movies(current_user)
    if favorite_movies:
        return jsonify({"favorite_movies": favorite_movies[0]})
    else:
        return jsonify({"error": "User not found"}), 404

def update_favorite_movies(current_user):
    data = request.json
    favorite_movies = data.get('favorite_movies')

    if favorite_movies is None:
        return jsonify({"error": "Invalid input"}), 400

    db_operations.update_favorite_movies(current_user, favorite_movies)
    return jsonify({"message": "Favorite movies updated successfully"})

def remove_favorite_movie(current_user):
    data = request.json
    movie_id = data.get('movie_id')

    if not movie_id:
        return jsonify({"error": "Invalid input"}), 400

    db_operations.remove_favorite_movie(current_user, movie_id)
    return jsonify({"message": "Favorite movie removed successfully"})

def search_movies():
    query = request.args.get('query')
    if not query:
        return jsonify({"error": "Query parameter is required"}), 400

    movies = db_operations.search_movies(query)
    movie_list = []
    for movie in movies:
        movie_dict = {
            "movie_id": movie[0],
            "movie_title": movie[1],
            "release_date": movie[2],
            "IMDb_URL": movie[3],
            "poster_url": movie[4]
        }
        movie_list.append(movie_dict)

    return jsonify(movie_list)

def get_ratings_count():
    ratings_count = db_operations.get_ratings_count()
    ratings_list = []
    for rating in ratings_count:
        rating_dict = {
            "movie_id": rating[0],
            "ratings_count": rating[1]
        }
        ratings_list.append(rating_dict)

    return jsonify(ratings_list)

def add_movie_rating(current_user):
    data = request.json
    movie_id = data.get('movie_id')
    rating = data.get('rating')

    if not movie_id or rating is None:
        return jsonify({"error": "Invalid input"}), 400

    db_operations.add_movie_rating(current_user, movie_id, rating)
    return jsonify({"message": "Rating added successfully"})

def interaction(current_user):
    data = request.get_json()
    movie_id = data.get('movie_id')
    interaction_type = data.get('interaction_type')
    duration = data.get('duration', 0)  # 默认为0

    if not movie_id or not interaction_type:
        return jsonify({"message": "movie_id and interaction_type are required"}), 400

    # logging.info(f"Logging interaction: user_id={current_user}, movie_id={movie_id}, interaction_type={interaction_type}, duration={duration}")

    db_operations.log_interaction(current_user, movie_id, interaction_type, duration)
    return jsonify({"message": "Interaction logged successfully"}), 200
