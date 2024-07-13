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

    # foreach()--action算子，对rdd的每个元素，执行你提供的逻辑的操作(和map一样)，但是这个方法没有返回值
    # foreach()--就是一个没有返回值的map
    # 且不同于top和collect等都是集中到driver中，损耗网络io。它直接在executor中执行，比统一到driver中性能更好
    result = rdd.foreach(lambda x: print(x * 10))
    print(result)  # None--说明没有返回值
    sc.stop()
