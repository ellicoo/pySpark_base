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
    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)

    # 定义操作规则，接收可迭代对象，同时返回的也是可迭代对象
    # 比如接收list，返回list
    # 以为是一整个分区的数据io过去进行计算的。需要进行迭代计算整个分区的所有元素，顾需要迭代器对象
    # 下面不还是一条条数据进行计算的吗？是的，但是只有一次网络IO，比如一个分区有一百万条数据，走map会大量的网络io
    # 走分区时，整个分区被打包成一个迭代器对象，就只有一个io
    # 在CPU执行层面我们没有省下来任何东西，但是在网络IO空间复杂度上要好多了

    def process(iter):
        result = list()
        for it in iter:
            result.append(it * 10)
        return result


    # 分区操作算子
    # mapPartitions与map功能一致，但是处理过程不一样，map一一处理，map是一次io，一整个分区都过去计算
    # mapPartitions() 接收一个自定义函数，与普通的map不一样。这个自定义函数接受的是一个迭代器对象，也返回一个迭代器对象
    # 该自定义函数参数可以把RDD打包成各个分区的可迭代对象
    print(rdd.mapPartitions(process).collect())
    sc.stop()