# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：groupBy&groupBy的用法
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

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 2), ('b', 3)])
    rdd.foreach(lambda x:print(x))

    print('---------------------------')
    # 通过groupBy对数据进行分组
    # groupBy传入的函数的 意思是: 通过这个函数, 确定按照谁来分组(返回谁即可)
    # 分组规则 和SQL是一致的, 也就是相同的在一个组(Hash分组)
    result = rdd.groupBy(lambda t: t[0]) # 返回的是对象，可以对每个元素进行处理--使用map
    # groupBy是根据各个元祖中('',1)中的第一个元素''即groupBy根据t[0]进行分组，然后返回分组好的新的rdd

    # result.foreach(lambda x: print(x))

    print('-----------------------------')
    #  想要将迭代器对象打出内容，使用map处理
    print(result.map(lambda t: (t[0], list(t[1]))).collect())

    print('------------或者--------------')
    result_rdd = rdd.groupByKey().mapValues(list)
    result_rdd.foreach(lambda x: print(x))
    # 5.关闭SparkContext
    sc.stop()


