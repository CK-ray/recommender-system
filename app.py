from flask import Flask, jsonify, request
from flask_cors import CORS
import db_operations
from config import Config
import jwt
import bcrypt
from functools import wraps
import datetime
import logging
import api_responses
from services.genre_distribution import genre_distribution_api
from services.rating_trend import rating_trend_api
from services.viewing_frequency import viewing_frequency_api
from services.viewing_time_period import viewing_frequency_by_time_period_api
from services.viewing_person import most_watched_directors_actors_api
from services.content_recommendation import content_based_recommendation
from services.hybrid_recommendation import hybrid_recommendation
from services.feedback import update_feedback
from services.conversational_qa import get_system_initiative, get_user_initiative
from cryptography.fernet import Fernet

app = Flask(__name__)
app.config.from_object(Config)
SECRET_KEY = app.config['SECRET_KEY']
ENCRYPTION_KEY = Config.ENCRYPTION_KEY
cipher = Fernet(ENCRYPTION_KEY)

CORS(app, resources={r"/*": {"origins": "*"}})


# JWT生成函数
def generate_token(user_id):
    payload = {
        'user_id': user_id,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(days=1)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')
    return token

# JWT验证装饰器
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            # print("Authorization header:", request.headers['Authorization'])
            token = request.headers['Authorization'].split(" ")[1]

        if not token:
            return jsonify({'message': 'Token is missing!'}), 401

        try:
            data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            # print("Decoded JWT data:", data)
            current_user = data['user_id']
            # print(f"Token valid, current_user: {current_user}")
        except jwt.ExpiredSignatureError:
            # print("Token has expired")
            return jsonify({'message': 'Token has expired!'}), 401
        except jwt.InvalidTokenError:
            # print("Token is invalid")
            return jsonify({'message': 'Token is invalid!'}), 401

        print("Token is valid, continuing with the request...")
        return f(current_user, *args, **kwargs)

    return decorated

@app.route('/api/main_carousel_movies', methods=['GET'])
def main_carousel_movies():
    return api_responses.get_main_carousel_movies()

@app.route('/api/recommended_movies', methods=['GET'])
def recommended_movies():
    return api_responses.get_recommended_movies()

@app.route('/api/latest_movies', methods=['GET'])
def latest_movies():
    return api_responses.get_latest_movies()

@app.route('/api/rating_recommendations', methods=['GET'])
def rating_recommendations():
    return api_responses.get_highest_rated_movies()

@app.route('/api/all_rated', methods=['GET'])
def top_rated():
    return api_responses.get_all_rated_movies()

@app.route('/api/movie/<int:movie_id>', methods=['GET'])
def movie_details(movie_id):
    return api_responses.get_movie_details(movie_id)

@app.route('/api/ratings/<int:movie_id>', methods=['GET'])
def movie_ratings(movie_id):
    return api_responses.get_movie_average_rating(movie_id)

@app.route('/api/trailer/<int:movie_id>', methods=['GET'])
def movie_trailer(movie_id):
    return api_responses.get_movie_trailer(movie_id)

@app.route('/api/categories', methods=['GET'])
def categories():
    return api_responses.get_all_categories()

@app.route('/api/movies/category/<string:category>', methods=['GET'])
def movies_by_category(category):
    return api_responses.get_movies_by_category(category)


@app.route('/api/user/genres', methods=['POST'])
@token_required
def update_genres(current_user):
    return api_responses.update_user_genres(current_user)

@app.route('/api/user/update', methods=['PUT'])
@token_required
def update_user_info(current_user):
    return api_responses.update_user_info(current_user)

@app.route('/api/user/<int:user_id>', methods=['GET'])
@token_required
def get_user_info(current_user, user_id):
    if current_user != user_id:
        return jsonify({'message': 'Access forbidden'}), 403
    return api_responses.get_user_info(user_id)

@app.route('/api/user/favorite_movies/<int:user_id>', methods=['GET'])
@token_required
def get_favorite_movies(current_user, user_id):
    if current_user != user_id:
        return jsonify({'message': 'Access forbidden'}), 403
    return api_responses.get_favorite_movies(user_id)

@app.route('/api/user/favorite_movies', methods=['PUT'])
@token_required
def update_favorite_movies(current_user):
    return api_responses.update_favorite_movies(current_user)

@app.route('/api/user/favorite_movies/remove', methods=['DELETE'])
@token_required
def remove_favorite_movie(current_user):
    return api_responses.remove_favorite_movie(current_user)

@app.route('/api/search', methods=['GET'])
def search():
    return api_responses.search_movies()

@app.route('/api/ratings', methods=['POST'])
@token_required
def add_rating(current_user):
    return api_responses.add_movie_rating(current_user)

@app.route('/api/ratings/count', methods=['GET'])
def ratings_count():
    return api_responses.get_ratings_count()

@app.route('/user/<int:user_id>/genre_distribution')
@token_required
def genre_distribution(current_user, user_id):
    print("Decoded JWT data in genre:", current_user)
    if current_user != user_id:
        return jsonify({'message': 'Access forbidden'}), 403
    return genre_distribution_api(user_id)

@app.route('/user/<int:user_id>/rating_trend')
@token_required
def rating_trend(current_user, user_id):
    if current_user != user_id:
        return jsonify({'message': 'Access forbidden'}), 403
    return rating_trend_api(user_id)

@app.route('/user/<int:user_id>/viewing_frequency')
@token_required
def viewing_frequency(current_user, user_id):
    if current_user != user_id:
        return jsonify({'message': 'Access forbidden'}), 403
    return viewing_frequency_api(user_id)

@app.route('/user/<int:user_id>/viewing_time_period')
@token_required
def viewing_frequency_time_period(current_user, user_id):
    if current_user != user_id:
        return jsonify({'message': 'Access forbidden'}), 403
    return viewing_frequency_by_time_period_api(user_id)

@app.route('/user/<int:user_id>/most_watched_directors_actors')
@token_required
def most_watched_directors_actors(current_user, user_id):
    if current_user != user_id:
        return jsonify({'message': 'Access forbidden'}), 403
    return most_watched_directors_actors_api(user_id)

@app.route('/user/<int:user_id>/content_based_recommendation', methods=['GET'])
@token_required
def content_based_recommendation_route(current_user, user_id):
    recommendations = content_based_recommendation(user_id)

    if isinstance(recommendations, str):
        # 如果 recommendations 是字符串，表示出现错误或没有推荐结果
        return jsonify({"error": recommendations}), 500

    try:
        # 确保 recommendations 是一个 DataFrame
        response = recommendations.to_dict(orient='records')
    except AttributeError as e:
        app.logger.error(f"Error converting recommendations to dict: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

    return jsonify(response)

@app.route('/user/<int:user_id>/hybrid_recommendation', methods=['GET'])
@token_required
def hybrid_recommendation_route(current_user, user_id):
    if current_user != user_id:
        return jsonify({'message': 'Access forbidden'}), 403
    try:
        recommendations = hybrid_recommendation(user_id)
        return jsonify(recommendations.to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Error in hybrid_recommendation_route: {str(e)}")
        return str(e), 500

@app.route('/api/feedback', methods=['POST'])
@token_required
def handle_feedback(current_user):
    data = request.json
    movie_id = data.get('movie_id')
    feedback = data.get('feedback')

    if not movie_id or feedback not in ['like', 'dislike']:
        return jsonify({'status': 'error', 'message': 'Invalid input'}), 400

    try:
        update_feedback(current_user, movie_id, feedback)
        return jsonify({'status': 'success', 'message': 'Feedback recorded successfully'}), 200
    except Exception as e:
        logging.error(f"Error updating feedback: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system_initiative', methods=['GET'])
def system_initiative():
    return get_system_initiative()

@app.route('/api/user_initiative', methods=['GET'])
def user_initiative():
    return get_user_initiative()


@app.route('/api/login', methods=['POST'])
def login_user():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    user = db_operations.get_user_by_username(username)
    if user is None:
        return jsonify({"error": "Invalid username"}), 401
    elif not bcrypt.checkpw(password.encode('utf-8'), user[2].encode('utf-8')):
        return jsonify({"error": "Invalid password"}), 401

    token = generate_token(user[0])
    user_info = {
        "user_id": user[0],
        "username": user[1],
        "email": user[3],
        "preferred_genres": user[4]
    }
    return jsonify({'token': token, 'user': user_info})




@app.route('/api/register', methods=['POST'])
def register_user():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')

    # 先检查是否存在相同的用户名
    existing_user = db_operations.get_user_by_username(username)
    if existing_user:
        return jsonify({"error": "Username already exists"}), 409

    # 哈希化密码
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    db_operations.insert_new_user(username, hashed_password.decode('utf-8'), email)
    return jsonify({"message": "User registered successfully"})


@app.route('/api/interaction', methods=['POST'])
@token_required
def interaction(current_user):
    return api_responses.interaction(current_user)






if __name__ == '__main__':
    app.run(debug=True, port=5001)
