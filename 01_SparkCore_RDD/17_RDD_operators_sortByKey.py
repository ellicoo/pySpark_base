# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：sortByKey
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

    rdd = sc.parallelize([('a', 1), ('E', 1), ('C', 1), ('D', 1), ('b', 1), ('g', 1), ('f', 1),
                          ('y', 1), ('u', 1), ('i', 1), ('o', 1), ('p', 1),
                          ('m', 1), ('n', 1), ('j', 1), ('k', 1), ('l', 1)], 3)
    # 不想让大小写影响就把key转小写--str(key).lower()
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())

    print('------------------------------------------------')

    # 2.数据输入
    input_rdd = sc.textFile(get_absolute_path("../data/input/time_music_party.csv"))

    # 3.数据处理
    # sortBy算子，适用于非KV类型的RDD
    result_rdd = input_rdd.sortBy(lambda line: line.split(",")[1], ascending=False)

    # 4.数据输出
    print("============1.分区数量==============")
    print(result_rdd.getNumPartitions())
    print("============2.分区内的元素==============")
    result_rdd.glom().foreach(lambda x: print(x))
    print("============3.所有元素==============")
    result_rdd.foreach(lambda x: print(x))

    # 5.关闭SparkContext
    sc.stop()