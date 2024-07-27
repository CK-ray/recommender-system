import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# 创建数据库连接
engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')

def get_user_ratings(user_id):
    query = """
        SELECT r.movie_id, r.rating, m.*
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        WHERE r.user_id = %s
    """
    user_ratings = pd.read_sql_query(query, engine, params=(user_id,))

    # 移除重复的列标签
    user_ratings = user_ratings.loc[:,~user_ratings.columns.duplicated()]

    return user_ratings

def get_all_movies():
    query = "SELECT * FROM movies"
    all_movies = pd.read_sql_query(query, engine)
    all_movies = all_movies.drop_duplicates(subset=['movie_id'])  # 移除重复的 movie_id

    return all_movies

def get_user_preferences(user_id):
    query = "SELECT preferred_genres, favorite_movies FROM users WHERE user_id = %s"
    user_preferences = pd.read_sql_query(query, engine, params=(user_id,))
    return user_preferences.iloc[0]

def create_movie_features(movies):
    # 将列名中的空格替换为下划线
    movies.columns = movies.columns.str.replace(' ', '_')
    # 注意确保列名与数据库中的一致
    features = movies[
        ['unknown', 'Action', 'Adventure', 'Animation', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Family',
         'Fantasy', 'History', 'Horror', 'Music', 'Mystery', 'Romance', 'Science_Fiction', 'TV_Movie', 'Thriller',
         'War', 'Western']]
    features.index = movies['movie_id']
    return features

def compute_user_profile(user_ratings, movie_features, preferred_genres, favorite_movies):
    # 移除 user_ratings 中的重复 movie_id
    user_ratings = user_ratings.drop_duplicates(subset=['movie_id'])

    # 获取用户评分中的电影ID
    user_movie_ids = user_ratings['movie_id'].values.flatten()

    # 确保 movie_features 的索引没有重复的 movie_id
    movie_features = movie_features[~movie_features.index.duplicated(keep='first')]

    # 过滤 movie_features 以仅包含用户评分中存在的电影ID
    user_movie_features = movie_features.loc[movie_features.index.intersection(user_movie_ids)]

    # 使用 merge 方法对齐 user_ratings 和 user_movie_features
    user_ratings_aligned = user_ratings[['movie_id', 'rating']].merge(user_movie_features, left_on='movie_id', right_index=True)

    user_profile = np.dot(user_ratings_aligned['rating'].values, user_ratings_aligned.drop(['movie_id', 'rating'], axis=1).values)
    user_profile = user_profile / np.sum(user_ratings_aligned['rating'].values)  # 归一化用户画像

    # 考虑用户偏好类型
    genre_columns = movie_features.columns
    genre_weights = np.zeros(len(genre_columns))

    for genre in preferred_genres.split(','):
        genre = genre.strip()
        if genre in genre_columns:
            genre_index = genre_columns.get_loc(genre)
            genre_weights[genre_index] = 1

    genre_weights = genre_weights / np.sum(genre_weights)  # 归一化偏好权重
    user_profile = (user_profile + genre_weights) / 2  # 将偏好类型权重与用户画像合并

    # 考虑用户喜好电影
    if favorite_movies:
        favorite_movie_ids = [int(id.strip()) for id in favorite_movies.split(',')]
    else:
        favorite_movie_ids = []

    favorite_movie_features = movie_features.loc[movie_features.index.intersection(favorite_movie_ids)]

    if not favorite_movie_features.empty:
        favorite_profile = favorite_movie_features.mean(axis=0).values
        user_profile = (user_profile + favorite_profile) / 2  # 将喜好电影的特征与用户画像合并

    return user_profile

def recommend_movies(user_profile, all_movies, movie_features, user_rated_movie_ids, top_n=6):
    similarities = cosine_similarity([user_profile], movie_features)[0]
    all_movies['similarity'] = similarities
    recommended_movies = all_movies[~all_movies['movie_id'].isin(user_rated_movie_ids)].sort_values(by='similarity', ascending=False).head(top_n)
    return recommended_movies

def content_based_recommendation(user_id):
    user_ratings = get_user_ratings(user_id)
    all_movies = get_all_movies()
    user_preferences = get_user_preferences(user_id)

    if user_ratings.empty:
        return "No ratings available for this user."

    movie_features = create_movie_features(all_movies)

    user_profile = compute_user_profile(user_ratings, movie_features, user_preferences['preferred_genres'], user_preferences['favorite_movies'])

    recommended_movies = recommend_movies(user_profile, all_movies, movie_features, user_ratings['movie_id'].values.flatten())

    return recommended_movies[['movie_id', 'movie_title', 'poster_url', 'similarity']]

# 调试代码以检查数据的状态
if __name__ == "__main__":
    user_id = 2  # 假设你有一个用户ID为2的用户
    recommendations = content_based_recommendation(user_id)
    print("Recommendations:")
    print(recommendations)
