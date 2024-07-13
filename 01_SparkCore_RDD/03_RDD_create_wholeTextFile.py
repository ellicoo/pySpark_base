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

    # 读取小文件文件夹--wholeTextFiles可以读取文件夹下的所有小文件
    rdd= sc.wholeTextFiles(get_absolute_path("../data/input/tiny_files"))
    # 通过wholeTextFiles读取的文件返回的rdd中的每个元素都是键值对的元祖对象。、

    print(rdd.collect()) # 通过collect收集到driver进行打印内容。返回的是键值对对象
    # 因为rdd的每个元素都是键值对对象，所以匿名函数的参数x接收到的是一个可迭代的元祖对象，所以可以使用索引下标取出value值
    print(rdd.map(lambda x:x[1]).collect()) # 要取出value值而不是键值对象

    # 5.关闭SparkContext
    sc.stop()