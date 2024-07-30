# config.py

import os
from dotenv import load_dotenv

load_dotenv()  # 加载 .env 文件中的环境变量

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'fecd3c5ef6d105ffbb8e4a2b16f94b7eabdd0b0c04f13d50b6586385609fb1fb')
    ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', 'AAxRCouMBDORDASgrYqV2tnOzU59827tHpN4lSjw8rs=')
# 其他配置项
