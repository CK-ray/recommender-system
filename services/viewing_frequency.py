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
    # 计算最近三个月的日期范围
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


def preprocess_viewing_data(user_data):
    # 将时间戳转换为日期格式
    user_data['timestamp'] = pd.to_datetime(user_data['timestamp'])
    # 按月聚合观影次数
    user_data['month'] = user_data['timestamp'].dt.to_period('M').dt.to_timestamp()
    monthly_viewings = user_data.groupby('month').size().reset_index(name='view_count')
    return monthly_viewings


def generate_viewing_frequency_barchart(monthly_viewings):
    # 如果没有观影数据，返回一个提示图像
    if monthly_viewings.empty:
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
    plt.bar(monthly_viewings['month'].astype(str), monthly_viewings['view_count'], color='skyblue')
    plt.title('Monthly Viewing Frequency')
    plt.xlabel('Month')
    plt.ylabel('View Count')
    plt.xticks(rotation=10)

    # 将图表保存到字节流中
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plt.close()

    return img


def viewing_frequency_api(user_id):
    try:
        # 提取用户最近三个月的观影数据
        user_data = get_recent_viewing_data(user_id)

        # 调试信息
        print(f"User {user_id} has {len(user_data)} viewing records in the last 3 months.")

        # 如果没有数据，打印并返回
        if user_data.empty:
            print(f"No viewing data for user {user_id} in the last 3 months.")
            return "No viewing data available for the specified period."

        # 数据预处理
        monthly_viewings = preprocess_viewing_data(user_data)
        print(f"Monthly viewings:\n{monthly_viewings.head()}")

        # 生成观影频率条形图
        img = generate_viewing_frequency_barchart(monthly_viewings)
        print(f"Barchart generated successfully.")

        return send_file(img, mimetype='image/png')
    except Exception as e:
        return str(e)
