import pandas as pd
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np
import collections

# 创建数据库连接
engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')

# 创建SparkSession
spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

def load_and_clean_data():
    # 从数据库加载评分数据
    query = "SELECT user_id, movie_id, rating FROM ratings"
    ratings = pd.read_sql_query(query, engine)

    # 数据清洗
    ratings = ratings.dropna()
    ratings = ratings[(ratings['rating'] >= 1) & (ratings['rating'] <= 10)]
    ratings = ratings.drop_duplicates(subset=['user_id', 'movie_id'])

    # 数据类型转换
    ratings['user_id'] = ratings['user_id'].astype(int)
    ratings['movie_id'] = ratings['movie_id'].astype(int)
    ratings['rating'] = ratings['rating'].astype(float)

    # 将Pandas DataFrame转换为Spark DataFrame
    ratings_spark = spark.createDataFrame(ratings)
    ratings_spark.cache()  # 缓存数据
    return ratings_spark

class ALSManual:
    def __init__(self, rank=10, maxIter=5, regParam=0.1, tol=1e-4, alpha=0.01):
        self.rank = rank
        self.maxIter = maxIter
        self.regParam = regParam
        self.tol = tol
        self.alpha = alpha  # 学习率

    def fit(self, ratings):
        self.ratings = ratings.collect()  # Collect to driver
        self.users = list(set([r['user_id'] for r in self.ratings]))
        self.items = list(set([r['movie_id'] for r in self.ratings]))
        self.num_users = len(self.users)
        self.num_items = len(self.items)

        # 初始化用户和物品因子
        self.user_factors = np.random.rand(self.num_users, self.rank)
        self.item_factors = np.random.rand(self.num_items, self.rank)

        # 创建用户和物品映射
        self.user_map = {user_id: i for i, user_id in enumerate(self.users)}
        self.item_map = {item_id: i for i, item_id in enumerate(self.items)}

        for i in range(self.maxIter):
            self.update()
            self.gradient_update()  # 引入梯度更新
            if self.compute_loss() < self.tol:
                break

    def update(self):
        user_ratings = collections.defaultdict(list)
        item_ratings = collections.defaultdict(list)

        for r in self.ratings:
            user_idx = self.user_map[r['user_id']]
            item_idx = self.item_map[r['movie_id']]
            rating = r['rating']
            user_ratings[user_idx].append((item_idx, rating))
            item_ratings[item_idx].append((user_idx, rating))

        # 更新用户因子
        for user_idx, ratings in user_ratings.items():
            A = np.zeros((self.rank, self.rank))
            b = np.zeros(self.rank)
            for item_idx, rating in ratings:
                item_factor = self.item_factors[item_idx]
                A += np.outer(item_factor, item_factor)
                b += rating * item_factor
            self.user_factors[user_idx] = np.linalg.solve(A + self.regParam * np.eye(self.rank), b)

        # 更新物品因子
        for item_idx, ratings in item_ratings.items():
            A = np.zeros((self.rank, self.rank))
            b = np.zeros(self.rank)
            for user_idx, rating in ratings:
                user_factor = self.user_factors[user_idx]
                A += np.outer(user_factor, user_factor)
                b += rating * user_factor
            self.item_factors[item_idx] = np.linalg.solve(A + self.regParam * np.eye(self.rank), b)

    def gradient_update(self):
        for r in self.ratings:
            user_idx = self.user_map[r['user_id']]
            item_idx = self.item_map[r['movie_id']]
            rating = r['rating']
            prediction = np.dot(self.user_factors[user_idx],
                                self.item_factors[item_idx])
            error = rating - prediction
            self.user_factors[user_idx]\
                += (self.alpha * error *
                    self.item_factors[item_idx])
            self.item_factors[item_idx] \
                += (self.alpha * error *
                    self.user_factors[user_idx])

    def compute_loss(self):
        loss = 0
        for r in self.ratings:
            user_idx = self.user_map[r['user_id']]
            item_idx = self.item_map[r['movie_id']]
            rating = r['rating']
            prediction = np.dot(self.user_factors[user_idx], self.item_factors[item_idx])
            loss += (rating - prediction) ** 2
        loss += self.regParam * (np.sum(self.user_factors ** 2) + np.sum(self.item_factors ** 2))
        return loss

    def transform(self, test_data):
        predictions = []
        for row in test_data.collect():
            user_id = row['user_id']
            movie_id = row['movie_id']
            rating = row['rating']
            prediction = self.predict(user_id, movie_id)
            if prediction is not None:  # 忽略 None 值
                predictions.append((user_id, movie_id, rating, prediction))

        schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("movie_id", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("prediction", FloatType(), True)
        ])

        return spark.createDataFrame(predictions, schema)

    def predict(self, user_id, item_id):
        if user_id in self.user_map and item_id in self.item_map:
            user_idx = self.user_map[user_id]
            item_idx = self.item_map[item_id]
            return float(np.dot(self.user_factors[user_idx], self.item_factors[item_idx]))
        else:
            return None

def als_model_train():
    # 加载并清洗数据
    ratings_spark = load_and_clean_data()

    # 分割数据为训练集和测试集
    training, test = ratings_spark.randomSplit([0.8, 0.2])

    # 构建手动ALS模型
    als_manual = ALSManual(maxIter=5, regParam=0.1)
    als_manual.fit(training)

    # 在测试集上进行预测
    predictions = als_manual.transform(test)

    # 评估模型
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print(f"Root-mean-square error (RMSE) = {rmse}")

    # 返回模型实例
    model = als_manual
    return model

def als_recommend(user_id, model, n_recommendations=6):
    user_idx = model.user_map.get(user_id)
    if user_idx is None:
        return []

    user_vector = model.user_factors[user_idx]
    scores = model.item_factors.dot(user_vector)
    item_indices = np.argsort(scores)[-n_recommendations:][::-1]

    recommendations = [model.items[i] for i in item_indices]
    return recommendations

def main():
    model = als_model_train()
    user_id = 6  # 假设测试用户ID为6
    recommendations = als_recommend(user_id, model, n_recommendations=6)
    print(f"Recommendations for user {user_id}: {recommendations}")

if __name__ == "__main__":
    main()
