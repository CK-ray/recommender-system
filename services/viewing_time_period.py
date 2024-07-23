import pandas as pd
from sqlalchemy import create_engine
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
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
            r.timestamp 
        FROM ratings r
        WHERE r.user_id = %s AND r.timestamp BETWEEN %s AND %s
    """
    user_data = pd.read_sql_query(query, engine, params=(user_id, start_date, end_date))
    return user_data


def preprocess_viewing_data_by_time_period(user_data):
    # 将时间戳转换为日期时间格式
    user_data['timestamp'] = pd.to_datetime(user_data['timestamp'])
    # 提取小时信息
    user_data['hour'] = user_data['timestamp'].dt.hour

    # 根据小时划分时间段
    bins = [0, 6, 12, 18, 24]
    labels = ['Midnight to 6 AM', '6 AM to Noon', 'Noon to 6 PM', '6 PM to Midnight']
    user_data['time_period'] = pd.cut(user_data['hour'], bins=bins, labels=labels, right=False)

    # 按时间段聚合观影次数
    time_period_viewings = user_data.groupby('time_period').size().reset_index(name='view_count')
    return time_period_viewings


def generate_viewing_frequency_barchart_by_time_period(time_period_viewings):
    # 如果没有观影数据，返回一个提示图像
    if time_period_viewings.empty:
        img = io.BytesIO()
        plt.figure(figsize=(12, 6))
        plt.text(0.5, 0.5, 'No Viewing Data Available', horizontalalignment='center', verticalalignment='center',
                 fontsize=20, color='gray')
        plt.axis('off')
        plt.savefig(img, format='png')
        img.seek(0)
        plt.close()
        return img

    # 生成条形图
    plt.figure(figsize=(12, 6))
    plt.bar(time_period_viewings['time_period'], time_period_viewings['view_count'], color='skyblue')
    plt.title('Viewing Frequency by Time Period')
    plt.xlabel('Time Period')
    plt.ylabel('View Count')
    plt.xticks(rotation=10)

    # 将图表保存到字节流中
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plt.close()

    return img


def viewing_frequency_by_time_period_api(user_id):
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
        time_period_viewings = preprocess_viewing_data_by_time_period(user_data)
        print(f"Time period viewings:\n{time_period_viewings.head()}")

        # 生成观影频率条形图（按时间段）
        img = generate_viewing_frequency_barchart_by_time_period(time_period_viewings)
        print(f"Barchart by time period generated successfully.")

        return send_file(img, mimetype='image/png')
    except Exception as e:
        return str(e)
