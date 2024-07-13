# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：groupByKey&collect
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

# groupByKey只分组不聚合

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])
    # groupBy：需要指定分组元素--分组规则需要指定
    # groupByKey：自动对KV型RDD自动安装key进行分组--即它的分组规则是自动化的，不需要指定
    rdd2 = rdd.groupByKey()
    # 想要将迭代器对象打出内容，使用map处理
    print(rdd2.map(lambda x: (x[0], list(x[1]))).collect())
    # 与groupBy不同的是，生成的列表只保留值。
    # groupBy完全保留，形成元祖列表[('b', [('b', 1), ('b', 2), ('b', 3)]), ('a', [('a', 1), ('a', 1)])]

    # groupByKey与reduceByKey的区别
    # 1）功能不同
    # groupByKey--仅分组，不聚合
    # reduceByKey--既分组，又聚合
    # 2）性能不同：
    # groupByKey性能 << reduceByKey性能
    # 因为先分组后聚合，导致groupByKey的网络IO的大，而reduceByKey自带聚合逻辑，先在分区内做局部聚合，然后再走分组流程(shutffle)
    # 最后再做最终聚合，从而被shutfle的数据可以极大的减少，提高性能

    # 应用场景：
    # 当我们既要分组又要聚合时：方法1：groupByKey + reduce 两个算子实现,  方法2：直接reduceByKey(首选)，因为在shutfule分组之前，自带局部聚合

    sc.stop()


