# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：action算子开始
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

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 1)

    # takeOrdered(参数1，参数2)-- 对rdd进行排序取出前N个元素,与top的区别是：top只降序，但takeOrdered(n)可以升序和降序
    # rdd.takeordered() -- action算子
    # 参数1：要几个数据
    # 参数2：对排序的数据进行更改(不会更改数据本身，只是在排序的时候换个样子)
    #       这个方法使用安装元素自然顺序升序排序，如果你想玩倒叙，需要参数2 来对排序的数据进行处理

    # 升序
    print(rdd.takeOrdered(3))
    # 降序--需要对key一一处理，升序排序1最小，9最大，负数时，
    # -1最小大-9最小，实现降序--底层是升序，影响排序依据，不影响排序本身
    print(rdd.takeOrdered(3, lambda x: -x))
    sc.stop()
