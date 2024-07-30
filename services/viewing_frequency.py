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


def get_recent_viewing_data(user_id, months=3):  # 保持最近三个月的日期范围
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
    print(f"Fetched data for user {user_id} from {start_date} to {end_date}:")
    print(user_data)
    return user_data


def preprocess_viewing_data(user_data):
    user_data['timestamp'] = pd.to_datetime(user_data['timestamp'])
    print("Converted timestamps:")
    print(user_data['timestamp'])

    user_data['week'] = user_data['timestamp'].dt.to_period('W').dt.to_timestamp()
    print("Data with week column:")
    print(user_data[['timestamp', 'week']])

    weekly_viewings = user_data.groupby('week').size().reset_index(name='view_count')
    print("Weekly viewings:")
    print(weekly_viewings)
    return weekly_viewings


def generate_viewing_frequency_barchart(weekly_viewings):
    if weekly_viewings.empty:
        img = io.BytesIO()
        plt.figure(figsize=(12, 6))
        plt.text(0.5, 0.5, 'No Viewing Data Available', horizontalalignment='center', verticalalignment='center',
                 fontsize=20, color='gray')
        plt.axis('off')
        plt.savefig(img, format='png')
        img.seek(0)
        plt.close()
        return img

    plt.figure(figsize=(12, 6))
    plt.bar(weekly_viewings['week'].astype(str), weekly_viewings['view_count'], color='skyblue')
    plt.title('Weekly Viewing Frequency')
    plt.xlabel('Week')
    plt.ylabel('View Count')
    plt.xticks(rotation=20)

    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plt.close()

    return img


def viewing_frequency_api(user_id):
    try:
        user_data = get_recent_viewing_data(user_id)

        print(f"User {user_id} has {len(user_data)} viewing records in the last 3 months.")

        if user_data.empty:
            print(f"No viewing data for user {user_id} in the last 3 months.")
            return "No viewing data available for the specified period."

        weekly_viewings = preprocess_viewing_data(user_data)
        print(f"Weekly viewings: {weekly_viewings}")

        img = generate_viewing_frequency_barchart(weekly_viewings)
        print(f"Barchart generated successfully.")

        return send_file(img, mimetype='image/png')
    except Exception as e:
        print(f"Error generating viewing frequency chart: {e}")
        return str(e)
