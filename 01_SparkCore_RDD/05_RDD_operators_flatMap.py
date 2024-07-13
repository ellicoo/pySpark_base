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

    rdd = sc.parallelize(["hadoop spark hadoop", "spark hadoop hadoop", "hadoop flink spark"])
    # 得到所有的单词, 组成RDD, flatMap的传入参数 和map一致, 就是给map逻辑用的, 解除嵌套无需逻辑(传参)
    rdd2 = rdd.flatMap(lambda line: line.split(" "))
    print(rdd2.collect())
    # 5.关闭SparkContext
    sc.stop()

# 使用flatMap将子列表展平--如果使用parallelize创建的并行集合，则flatmap主要的功能是去嵌套
# 如果使用textFile读出的rdd，flatmap主要的功能是去换行符号
# 说人话就是：打散元素

# map则对打散的元素进行一对一的处理成（键，值）--> （'我'，1）