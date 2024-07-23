import pandas as pd
from sqlalchemy import create_engine
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import io
from flask import send_file
from datetime import datetime, timedelta

# 创建数据库连接
engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')


def get_recent_viewing_data(user_id, months=6):
    # 计算最近六个月的日期范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 30)

    query = """
        SELECT 
            r.user_id, 
            r.movie_id, 
            m.director, 
            m.cast, 
            r.timestamp 
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        WHERE r.user_id = %s AND r.timestamp BETWEEN %s AND %s
    """
    user_data = pd.read_sql_query(query, engine, params=(user_id, start_date, end_date))
    return user_data


def preprocess_directors_and_actors(user_data):
    # 拆分导演和演员信息，并统计出现次数
    director_counts = user_data['director'].str.split(',').explode().str.strip().value_counts()
    actor_counts = user_data['cast'].str.split(',').explode().str.strip().value_counts()
    return director_counts, actor_counts


def generate_combined_wordcloud(director_counts, actor_counts):
    # 如果没有数据，返回一个提示图像
    if director_counts.empty and actor_counts.empty:
        img = io.BytesIO()
        plt.figure(figsize=(12, 6))
        plt.text(0.5, 0.5, 'No Data Available', horizontalalignment='center', verticalalignment='center', fontsize=20,
                 color='gray')
        plt.axis('off')
        plt.savefig(img, format='png')
        img.seek(0)
        plt.close()
        return img

    # 生成导演词云图
    wordcloud_directors = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(
        director_counts)

    # 生成演员词云图
    wordcloud_actors = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(
        actor_counts)

    # 生成组合词云图
    fig, axs = plt.subplots(1, 2, figsize=(20, 10))

    axs[0].imshow(wordcloud_directors, interpolation='bilinear')
    axs[0].set_title('Most Watched Directors')
    axs[0].axis('off')

    axs[1].imshow(wordcloud_actors, interpolation='bilinear')
    axs[1].set_title('Most Watched Actors')
    axs[1].axis('off')

    # 将图表保存到字节流中
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plt.close()

    return img


def most_watched_directors_actors_api(user_id):
    try:
        # 提取用户最近六个月的观影数据
        user_data = get_recent_viewing_data(user_id)

        # 调试信息
        print(f"User {user_id} has {len(user_data)} viewing records in the last 6 months.")

        # 如果没有数据，打印并返回
        if user_data.empty:
            print(f"No viewing data for user {user_id} in the last 6 months.")
            return "No viewing data available for the specified period."

        # 数据预处理
        director_counts, actor_counts = preprocess_directors_and_actors(user_data)
        print(f"Director counts:\n{director_counts.head()}")
        print(f"Actor counts:\n{actor_counts.head()}")

        # 生成导演和演员的组合词云图
        img = generate_combined_wordcloud(director_counts, actor_counts)
        print(f"Combined wordcloud generated successfully.")

        return send_file(img, mimetype='image/png')
    except Exception as e:
        return str(e)
