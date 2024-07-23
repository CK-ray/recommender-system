from flask import Flask, jsonify, request
from flask_cors import CORS
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

app = Flask(__name__)
CORS(app)

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

@app.route('/api/login', methods=['POST'])
def login():
    return api_responses.login_user()

@app.route('/api/register', methods=['POST'])
def register():
    return api_responses.register_user()

@app.route('/api/user/genres', methods=['POST'])
def update_genres():
    return api_responses.update_user_genres()

@app.route('/api/user/update', methods=['PUT'])
def update_user_info():
    return api_responses.update_user_info()

@app.route('/api/user/<int:user_id>', methods=['GET'])
def get_user_info(user_id):
    return api_responses.get_user_info(user_id)


@app.route('/api/user/favorite_movies/<int:user_id>', methods=['GET'])
def get_favorite_movies(user_id):
    return api_responses.get_favorite_movies(user_id)

@app.route('/api/user/favorite_movies', methods=['PUT'])
def update_favorite_movies():
    return api_responses.update_favorite_movies()

@app.route('/api/user/favorite_movies/remove', methods=['DELETE'])
def remove_favorite_movie():
    return api_responses.remove_favorite_movie()

@app.route('/api/search', methods=['GET'])
def search():
    return api_responses.search_movies()

@app.route('/api/ratings', methods=['POST'])
def add_rating():
    return api_responses.add_movie_rating()

@app.route('/api/ratings/count', methods=['GET'])
def ratings_count():
    return api_responses.get_ratings_count()

@app.route('/user/<int:user_id>/genre_distribution')
def genre_distribution(user_id):
    return genre_distribution_api(user_id)


@app.route('/user/<int:user_id>/rating_trend')
def rating_trend(user_id):
    return rating_trend_api(user_id)


@app.route('/user/<int:user_id>/viewing_frequency')
def viewing_frequency(user_id):
    return viewing_frequency_api(user_id)

@app.route('/user/<int:user_id>/viewing_time_period')
def viewing_frequency_time_period(user_id):
    return viewing_frequency_by_time_period_api(user_id)


@app.route('/user/<int:user_id>/most_watched_directors_actors')
def most_watched_directors_actors(user_id):
    return most_watched_directors_actors_api(user_id)

@app.route('/user/<int:user_id>/content_based_recommendation')
def content_based_recommendation_route(user_id):
    recommendations = content_based_recommendation(user_id)
    return jsonify(recommendations.to_dict(orient='records'))

@app.route('/user/<int:user_id>/hybrid_recommendation', methods=['GET'])
def hybrid_recommendation_route(user_id):
    try:
        recommendations = hybrid_recommendation(user_id)
        return jsonify(recommendations.to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Error in hybrid_recommendation_route: {str(e)}")
        return str(e), 500

@app.route('/api/feedback', methods=['POST'])
def handle_feedback():
    data = request.json
    user_id = data.get('user_id')
    movie_id = data.get('movie_id')
    feedback = data.get('feedback')

    if not user_id or not movie_id or feedback not in ['like', 'dislike']:
        return jsonify({'status': 'error', 'message': 'Invalid input'}), 400

    try:
        update_feedback(user_id, movie_id, feedback)
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





if __name__ == '__main__':
    app.run(debug=True, port=5001)
