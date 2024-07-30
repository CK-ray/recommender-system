import pandas as pd
from sqlalchemy import create_engine
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
from flask import send_file


engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')


def genre_distribution_api(user_id):
    query = """
        SELECT 
            r.user_id, 
            r.movie_id, 
            r.rating, 
            r.timestamp, 
            m.movie_title, 
            m.Action, 
            m.Adventure, 
            m.Animation, 
            m.Comedy, 
            m.Crime, 
            m.Documentary, 
            m.Drama, 
            m.Family, 
            m.Fantasy, 
            m.History, 
            m.Horror, 
            m.Music, 
            m.Mystery, 
            m.Romance, 
            m.`Science Fiction`, 
            m.`TV Movie`, 
            m.Thriller, 
            m.War, 
            m.Western 
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        WHERE r.user_id = %s
    """

    try:
        # print("Executing query with user_id:", user_id)
        user_data = pd.read_sql_query(query, engine, params=(user_id,))
        # print("Query executed successfully")
    except Exception as e:
        # print("Error executing query:", e)
        return str(e)

    try:
        genre_columns = [
            'Action', 'Adventure', 'Animation', 'Comedy', 'Crime', 'Documentary', 'Drama',
            'Family', 'Fantasy', 'History', 'Horror', 'Music', 'Mystery', 'Romance',
            'Science Fiction', 'TV Movie', 'Thriller', 'War', 'Western'
        ]

        genre_counts = user_data[genre_columns].sum().sort_values(ascending=False)

        total_count = genre_counts.sum()
        genre_percentages = genre_counts / total_count * 100

        # The types with a percentage of less than or equal to 4% are classified as "Other Genres"
        other_genres_count = genre_counts[genre_percentages <= 4].sum()
        genre_counts = genre_counts[genre_percentages > 4]
        genre_counts['Other Genres'] = other_genres_count

        # Recalculate Percentage
        genre_percentages = genre_counts / genre_counts.sum() * 100

        # Generate chart
        plt.figure(figsize=(10, 6))
        genre_counts.plot.pie(autopct='%1.1f%%', startangle=140,
                              colors=plt.cm.Paired(range(len(genre_counts))))
        plt.title('Genre Distribution')
        plt.ylabel('')

        # Save the chart to a byte stream
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plt.close()

        return send_file(img, mimetype='image/png')
    except Exception as e:
        print("Error generating chart:", e)
        return str(e)
