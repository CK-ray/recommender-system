import pandas as pd
from sqlalchemy import create_engine
import matplotlib

matplotlib.use('Agg')  # 设置Matplotlib后端为Agg
import matplotlib.pyplot as plt
import io
from flask import send_file

# 创建数据库连接
engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')


def get_user_ratings(user_id):
    query = """
        SELECT 
            r.user_id, 
            r.movie_id, 
            r.rating, 
            r.timestamp 
        FROM ratings r
        WHERE r.user_id = %s
    """
    user_data = pd.read_sql_query(query, engine, params=(user_id,))
    return user_data


def preprocess_ratings_data(user_data, freq='M'):
    # 将时间戳转换为日期时间格式
    user_data['timestamp'] = pd.to_datetime(user_data['timestamp'])

    # 按选择的时间粒度进行分组，计算每个时间段的平均评分
    user_data.set_index('timestamp', inplace=True)
    ratings_resampled = user_data['rating'].resample('ME').mean()

    # 处理缺失值（这里使用前向填充作为示例）
    ratings_resampled.ffill(inplace=True)

    return ratings_resampled


def generate_rating_trend_chart(ratings_resampled):
    plt.figure(figsize=(10, 6))
    plt.plot(ratings_resampled.index, ratings_resampled.values, marker='o', linestyle='-')
    plt.title('Rating Trend Over Time')
    plt.xlabel('Time')
    plt.ylabel('Average Rating')
    plt.grid(True)

    # 将图表保存到字节流中
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plt.close()

    return img


def rating_trend_api(user_id):
    try:
        # 提取用户评分数据
        user_data = get_user_ratings(user_id)

        # 数据预处理
        ratings_resampled = preprocess_ratings_data(user_data, freq='M')

        # 生成评分趋势图表
        img = generate_rating_trend_chart(ratings_resampled)

        return send_file(img, mimetype='image/png')
    except Exception as e:
        return str(e)
