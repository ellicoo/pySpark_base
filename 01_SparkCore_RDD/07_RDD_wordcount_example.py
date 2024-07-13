# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：wholeTextFiles读取小文件--创建rdd对象
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1. 读取文件获取数据 构建RDD
    file_rdd = sc.textFile(get_absolute_path("../data/input/words.txt"))

    # 2. 通过flatMap API 取出所有的单词--打散元素
    word_rdd = file_rdd.flatMap(lambda x: x.split(" "))

    # 3. 将单词转换成元组, key是单词, value是1 -- 将打散的元素弄成可以聚合的键值对
    word_with_one_rdd = word_rdd.map(lambda word: (word, 1))

    # 4. 用reduceByKey 对单词进行分组并进行value的聚合 -- 将键值对聚合
    result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 5. 通过collect算子, 将rdd的数据收集到Driver中, 打印输出
    print(result_rdd.collect())
    # 5.关闭SparkContext
    sc.stop()
