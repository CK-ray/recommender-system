from sqlalchemy import create_engine, text

# 创建数据库连接
engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')

def update_feedback(user_id, movie_id, feedback):
    query = text("""
        REPLACE INTO feedback (user_id, movie_id, feedback)
        VALUES (:user_id, :movie_id, :feedback)
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {'user_id': user_id, 'movie_id': movie_id, 'feedback': feedback})
        conn.commit()  # 确保提交事务
        print(result.rowcount)  # 打印受影响的行数
        return result.rowcount

def handle_feedback(user_id, movie_id, feedback):
    if feedback not in ['like', 'dislike']:
        return {'status': 'error', 'message': 'Invalid feedback type'}

    rows_affected = update_feedback(user_id, movie_id, feedback)
    if rows_affected > 0:
        return {'status': 'success', 'message': 'Feedback received'}
    else:
        return {'status': 'error', 'message': 'No rows affected'}

# 调试代码
if __name__ == "__main__":
    user_id = 2
    movie_id = 1
    feedback = 'like'
    result = handle_feedback(user_id, movie_id, feedback)
    print(result)
