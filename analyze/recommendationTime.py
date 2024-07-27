import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# 创建数据库连接
engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')


def fetch_recommendation_times():
    query = "SELECT id, duration FROM recommendation_times"
    recommendation_times = pd.read_sql_query(query, engine)
    return recommendation_times


def plot_recommendation_times():
    # 获取数据
    data = fetch_recommendation_times()

    # 计算平均秒数
    avg_duration = data['duration'].mean()

    # 绘制折线图
    plt.figure(figsize=(10, 6))
    plt.plot(data['id'], data['duration'], marker='o', label='Each Time')

    # 添加平均秒数的虚线
    plt.axhline(y=avg_duration, color='r', linestyle='--', label='Avg Time')

    plt.title('Recommendation Duration per Entry')
    plt.xlabel('Entry ID')
    plt.ylabel('Duration (seconds)')
    plt.grid(True)

    # 设置横坐标为整数
    plt.xticks(data['id'])

    # 添加图例
    plt.legend()

    plt.savefig('recommendation_times_plot.png')  # 保存为图片文件
    plt.show()


# 调试代码以生成图表
if __name__ == "__main__":
    plot_recommendation_times()
