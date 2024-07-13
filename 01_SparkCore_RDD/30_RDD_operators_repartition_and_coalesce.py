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

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)

    # repartition(n)--转换类算子，返回rdd--底层调用coalesce，但repartition走shuffle，coalesce默认不走
    # ---对rdd修改分区(仅数量，不对分区规则重新修改)
    # 参数n为分区数

    # 【注意】：对分区的数量进行操作，一定要慎重
    #           一般情况下，我们写spark代码除了要求全局排序设置为1个分区外
    #           多数时候，所有API中关于分区相关的代码我们都不用太理会
    # 原因：1）会影响并行计算--内存迭代的并行管道数量
    #      2）分区增加，极大可能导致shuffle--因为根据mapreduce知道，shuffle才是性能的核心问题
    #               因为shuffle有状态计算，涉及到状态的获取，导致性能下降，没有shuffle则
    #               大部分都是无状态计算，大部分都是并行执行，效率高

    # 应用场景：最好不要用repartition，当我们进行排序时，减少分区为1使用，推荐coalesce
    print(rdd.repartition(1).getNumPartitions())
    #
    print(rdd.repartition(5).getNumPartitions())

    # coalesce 修改分区--也可调大分区，走shuffle即可
    # coalesce 调小分区不走shuffle

    print(rdd.coalesce(1).getNumPartitions())
    # coalesce调大分区时，需要shuffle=True
    print(rdd.coalesce(5, shuffle=True).getNumPartitions())
    sc.stop()