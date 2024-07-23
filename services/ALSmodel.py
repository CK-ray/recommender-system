import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row

# 创建数据库连接
engine = create_engine('mysql+pymysql://root:12345678@localhost/recommendation_db')

# 创建SparkSession
spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def als_model_train():
    # 从数据库加载评分数据
    query = "SELECT user_id, movie_id, rating FROM ratings"
    ratings = pd.read_sql_query(query, engine)

    # 将Pandas DataFrame转换为Spark DataFrame
    ratings_spark = spark.createDataFrame(ratings)

    # 分割数据为训练集和测试集
    training, test = ratings_spark.randomSplit([0.8, 0.2])

    # 构建ALS模型
    als = ALS(maxIter=10, regParam=0.1, userCol="user_id", itemCol="movie_id", ratingCol="rating", coldStartStrategy="drop")
    model = als.fit(training)

    # 在测试集上进行预测
    predictions = model.transform(test)

    # 评估模型
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print(f"Root-mean-square error (RMSE) = {rmse}")

    return model

def als_recommend(user_id, model, n_recommendations=6):
    # 创建用户DataFrame
    user_df = spark.createDataFrame([Row(user_id=user_id)])

    # 生成推荐
    user_recommendations = model.recommendForUserSubset(user_df, n_recommendations)
    recommendations = user_recommendations.collect()[0].recommendations

    return [row.movie_id for row in recommendations]
