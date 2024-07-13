# coding:utf8
import os
import time
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
import re
"""
-------------------------------------------------
   Description :	TODO：共享变量之累加器
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""

# 需求分析：
# 1）正常的单词进行单词计数
# 2）特殊字符统计出现多少个

# 共享变量--driver中的本地数据和executor中的rdd数据需要一起进行运算时使用
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1. 读取数据文件
    file_rdd = sc.textFile(get_absolute_path("../data/input/accumulator_broadcast_data.txt"))

    # 特殊字符的list定义--本地(特殊字符)集合
    # 因为需求，要在rdd中找到包含的特殊字符，从而确定要跟rdd产生关联，故要把本地的特殊字符列表变成广播变量
    abnormal_char = [",", ".", "!", "#", "$", "%"] # 只有6个变量而已弄成rdd是很浪费的，所以做广播变量

    # 2. 将特殊字符list 包装成广播变量
    broadcast = sc.broadcast(abnormal_char)

    # 3. 对特殊字符出现次数做累加, 累加使用累加器最好--分布式代码中累加东西需要累加器才能把累加结果反应到driver中的累加器
    acmlt = sc.accumulator(0)

    # 4. 数据处理, 先处理数据的空行, 在Python中有内容就是True None就是False--即没有单词也没有特殊字符的空白行
    # strip-- 去除字符串前后空格，把字符串本身返回。如果是空行，则返回None等于False
    lines_rdd = file_rdd.filter(lambda line: line.strip())

    # 5. 去除前后的空格
    data_rdd = lines_rdd.map(lambda line: line.strip())

    # 6. 对数据进行切分, 按照正则表达式切分, 因为空格分隔符某些单词之间是两个或多个空格
    # 正则表达式 \s+ 表示 不确定多少个空格, 最少一个空格--通过空格切分
    words_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))

    # 7. 当前words_rdd中有正常单词 也有特殊符号.
    # 现在需要过滤数据, 保留正常单词用于做单词计数, 在过滤 的过程中 对特殊符号做计数
    def filter_func(data):
        """过滤数据, 保留正常单词用于做单词计数, 在过滤 的过程中 对特殊符号做计数"""
        global acmlt
        # 取出广播变量中存储的特殊符号list
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            # 表示这个是 特殊字符
            acmlt += 1
            return False
        else:
            return True

    normal_words_rdd = words_rdd.filter(filter_func)
    # 8. 正常单词的单词计数逻辑
    result_rdd = normal_words_rdd.map(lambda x: (x, 1)).\
        reduceByKey(lambda a, b: a + b)

    print("正常单词计数结果: ", result_rdd.collect())
    print("特殊字符数量: ", acmlt)
    sc.stop()
