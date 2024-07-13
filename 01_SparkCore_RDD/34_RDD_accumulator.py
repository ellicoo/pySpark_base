# coding:utf8
import os
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

"""
-------------------------------------------------
   Description :	TODO：共享变量之累加器
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""
# 共享变量--driver中的本地数据和executor中的rdd数据需要一起进行运算时使用
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
    # 2个分区，每个分区是5个元素，map是作用在每个rdd元素元素上的，所以每个分区都执行5次map_func函数
    # 但driver和executor是分布式的，count在每个分区的操作都没影响driver中的count=0的结果，因为
    # 只复制了数据而不是给executor内存地址。因为driver和executor不在一个服务器(节点)内，所以不可能给内存地址
    # 所以出现：
    # 1
    # 2
    # 3
    # 4
    # 5
    # 1
    # 2
    # 3
    # 4
    # 5
    # 0
    # 最后一个是driver中的count
    # 需求：对map算子计算中的数据，进行计数累加。得到全部数据计算完后的累加结果。

    # Spark提供的累加器变量, 参数是初始值

    # 非rdd代码由driver执行
    count = 0 # 使用count时，map_func是两个

    acmlt = sc.accumulator(0)


    def map_func(data):
        # 普通实现：分区累加无法同步反应到driver中的count
        # global count
        # count += 1
        # print(count)

        # spark提供的累加器对象，进行的操作相当于把driver中的内存地址给executor一样。这不是真的
        global acmlt
        acmlt += 1
        # print(acmlt)

    rdd2 = rdd.map(map_func) # 这个rdd调用map后得到rdd2
    rdd2.cache() # 一定要在collect之前做，如果collect之后做，rdd2已经消失了，即在action算子之前做，否则将再从头溯源
    rdd2.collect() # rdd2则collect()到driver中了，触发map计算

    rdd3 = rdd2.map(lambda x: x) # 这个map啥也不干，只为了得到rdd3
    rdd3.collect()
    print(acmlt)
    #假如没有缓存时，acmlt会变成20，因为rdd2被重复利用，导致rdd2重新通过rdd.map(map_func)计算产生，所以变20
    # 因为执行到rdd2.collect()后，rdd和rdd2已经不存在了(过程数据)
    # print(count)
    sc.stop()

    # 累加器--由于内存隔离，通过累加器解决全局累加问题