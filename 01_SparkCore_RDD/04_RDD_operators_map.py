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

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

    # （1）定义方法, 作为算子的传入函数体
    def add(data):
        return data * 10


    print(rdd.map(add).collect())

    # （2）匿名函数，更简单的方式 是定义lambda表达式来写匿名函数
    # map不一定要映射成键值对，可以自定义
    print(rdd.map(lambda data: data * 10).collect())
    # 5.关闭SparkContext
    sc.stop()
"""
对于算子的接受函数来说, 两种方法都可以
lambda表达式 适用于 一行代码就搞定的函数体(匿名函数), 如果是多行, 需要定义独立的方法.
"""

