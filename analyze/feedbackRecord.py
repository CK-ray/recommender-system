import matplotlib.pyplot as plt
import io
from flask import send_file, jsonify

import db_operations


@app.route('/user/<int:user_id>/feedback_stats', methods=['GET'])
def get_feedback_stats(user_id):
    # 获取用户的反馈统计数据
    feedback_counts = db_operations.get_feedback_counts_by_user_id(user_id)

    if not feedback_counts:
        return jsonify({"error": "No feedback found for user"}), 404

    # 生成饼状图
    labels = list(feedback_counts.keys())
    sizes = list(feedback_counts.values())
    colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99']

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    # 将图表保存到内存缓冲区
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)

    return send_file(img, mimetype='image/png')


# db_operations.py

