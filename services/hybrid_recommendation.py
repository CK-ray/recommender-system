import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import time
from services.ALSmodel import als_model_train, als_recommend
from sqlalchemy.orm import sessionmaker

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
    user_ratings = user_ratings.loc[:, ~user_ratings.columns.duplicated()]

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


def get_user_feedback(user_id):
    query = "SELECT movie_id, feedback FROM feedback WHERE user_id = %s"
    user_feedback = pd.read_sql_query(query, engine, params=(user_id,))
    return user_feedback


def get_user_interactions(user_id):
    query = "SELECT movie_id, interaction_type, duration FROM user_interactions WHERE user_id = %s"
    user_interactions = pd.read_sql_query(query, engine, params=(user_id,))
    return user_interactions


def create_movie_features(movies):
    movies.columns = movies.columns.str.replace(' ', '_')
    # 注意确保列名与数据库中的一致
    features = movies[
        ['unknown', 'Action', 'Adventure', 'Animation', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Family',
         'Fantasy', 'History', 'Horror', 'Music', 'Mystery', 'Romance', 'Science_Fiction', 'TV_Movie', 'Thriller',
         'War', 'Western']]
    features.index = movies['movie_id']
    return features


def compute_user_profile(user_ratings, movie_features, user_interactions, preferred_genres, favorite_movies):
    # 移除 user_ratings 中的重复 movie_id
    user_ratings = user_ratings.drop_duplicates(subset=['movie_id'])

    # 获取用户评分中的电影ID
    user_movie_ids = user_ratings['movie_id'].values.flatten()

    # 确保 movie_features 的索引没有重复的 movie_id
    movie_features = movie_features[~movie_features.index.duplicated(keep='first')]

    # 过滤 movie_features 以仅包含用户评分中存在的电影ID
    user_movie_features = movie_features.loc[movie_features.index.intersection(user_movie_ids)]

    # 使用 merge 方法对齐 user_ratings 和 user_movie_features
    user_ratings_aligned = user_ratings[['movie_id', 'rating']].merge(user_movie_features, left_on='movie_id',
                                                                      right_index=True)

    if len(user_ratings_aligned) == 1:
        user_profile = user_ratings_aligned.drop(['movie_id', 'rating'], axis=1).values[0]
    else:
        user_profile = np.dot(user_ratings_aligned['rating'].values,
                              user_ratings_aligned.drop(['movie_id', 'rating'], axis=1).values)
        user_profile = user_profile / np.sum(user_ratings_aligned['rating'].values)  # 归一化用户画像

    # 考虑用户点击和浏览时长
    for _, interaction in user_interactions.iterrows():
        movie_id = interaction['movie_id']
        if movie_id in movie_features.index:
            weight = 1.0
            if interaction['interaction_type'] == 'click':
                weight = 1.0
            elif interaction['interaction_type'] == 'view':
                weight = interaction['duration'] / 60.0  # 将浏览时长转换为分钟作为权重
            user_profile += weight * movie_features.loc[movie_id].values

    user_profile = user_profile / (len(user_interactions) + 1)  # 归一化用户画像

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


def recommend_movies(user_profile, all_movies, movie_features, user_rated_movie_ids, user_feedback, top_n=16):
    similarities = cosine_similarity([user_profile], movie_features)[0]
    all_movies['similarity'] = similarities

    # 根据用户反馈调整推荐结果
    for _, row in user_feedback.iterrows():
        if (row['movie_id'] in all_movies['movie_id'].values):
            if row['feedback'] == 'like':
                all_movies.loc[all_movies['movie_id'] == row['movie_id'], 'similarity'] *= 1.5
            elif row['feedback'] == 'dislike':
                all_movies.loc[all_movies['movie_id'] == row['movie_id'], 'similarity'] *= 0.5

    recommended_movies = all_movies[~all_movies['movie_id'].isin(user_rated_movie_ids)].sort_values(by='similarity',
                                                                                                    ascending=False).head(
        top_n)
    return recommended_movies


def content_based_recommendation(user_id):
    user_ratings = get_user_ratings(user_id)
    all_movies = get_all_movies()
    user_preferences = get_user_preferences(user_id)
    user_feedback = get_user_feedback(user_id)

    if user_ratings.empty:
        return "No ratings available for this user."

    movie_features = create_movie_features(all_movies)

    user_profile = compute_user_profile(user_ratings, movie_features, user_preferences['preferred_genres'],
                                        user_preferences['favorite_movies'])

    recommended_movies = recommend_movies(user_profile, all_movies, movie_features,
                                          user_ratings['movie_id'].values.flatten(), user_feedback)

    return recommended_movies[['movie_id', 'movie_title', 'poster_url', 'similarity']]


def hybrid_recommendation(user_id):
    start_time = time.time()  # 记录开始时间

    user_ratings = get_user_ratings(user_id)
    all_movies = get_all_movies()
    user_preferences = get_user_preferences(user_id)
    user_feedback = get_user_feedback(user_id)
    user_interactions = get_user_interactions(user_id)

    if user_ratings.empty:
        return "No ratings available for this user."

    movie_features = create_movie_features(all_movies)

    user_profile = compute_user_profile(user_ratings, movie_features, user_interactions,
                                        user_preferences['preferred_genres'], user_preferences['favorite_movies'])

    content_recommendations = recommend_movies(user_profile, all_movies, movie_features,
                                               user_ratings['movie_id'].values.flatten(), user_feedback)

    model = als_model_train()
    als_recommendations = als_recommend(user_id, model)

    als_recommendations = all_movies[all_movies['movie_id'].isin(als_recommendations)].head(16)

    hybrid_recommendations = pd.concat([content_recommendations, als_recommendations]).drop_duplicates(
        subset=['movie_id']).head(16)

    end_time = time.time()  # 记录结束时间
    duration = round(end_time - start_time, 2)  # 计算并四舍五入到两位小数

    print(f"Recommendation duration for user {user_id}: {duration} seconds")  # 调试信息
    record_recommendation_time(user_id, duration)  # 记录推荐时间

    return hybrid_recommendations[['movie_id', 'movie_title', 'poster_url', 'similarity']]




Session = sessionmaker(bind=engine)
session = Session()

def record_recommendation_time(user_id, duration):
    insert_query = text("""
        INSERT INTO recommendation_times (user_id, duration)
        VALUES (:user_id, :duration)
    """)
    try:
        print(f"Inserting user_id: {user_id}, duration: {duration}")
        session.execute(insert_query, {"user_id": user_id, "duration": duration})
        session.commit()
        print("Insert successful")
    except Exception as e:
        session.rollback()
        print(f"Insert failed: {e}")
    finally:
        session.close()




# 调试代码以检查数据的状态
if __name__ == "__main__":
    user_id = 2  # 假设你有一个用户ID为2的用户
    recommendations = hybrid_recommendation(user_id)
    print("Recommendations:")
    print(recommendations)
