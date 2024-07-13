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
    # 下面不还是一条条数据进行计算的吗？是的，但是只有一次网络IO，比如一个分区有一百万条数据，走map会大量的网络io
    # 走分区时，整个分区被打包成一个迭代器对象，就只有一个io
    # 在CPU执行层面我们没有省下来任何东西，但是在网络IO空间复杂度上要好多了
    def process(iter):
        result = list()
        for it in iter:
            result.append(it * 10)

        print(result)

    # 与foreach功能一样：foreach是一条一条，一样不接受返回值，而foreachPartition是对整个分区进行操作的
    rdd.foreachPartition(process)
    sc.stop()
