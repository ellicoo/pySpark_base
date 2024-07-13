# coding:utf8
import time
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
import os


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
    # rdd是过程数据产生的性能问题：
    # rdd的血缘关系：rdd是过程数据，只在处理的过程中存在，一旦处理完成，就不见了，这个特性可以最大化的利用资源
    # 老旧rdd没用了，就从内存中清理，给后续的计算腾出内存空间。当我们需要多次使用某个rdd时，需要从头执行产生这个rdd才能使用，产生该rdd链条会执行多次，性能差

    # 解决办法--rdd的缓存持久化--如果需要重复使用的某个rdd的数据，只需要保证该rdd的数据不被释放，这样就可以不用多次执行产生该rdd的计算链条

    # rdd持久化应用场景：当我们计算生成某个比如rdd3，且确定该rdd在后续计算中需要重复使用时，当rdd4生成时，我们需要告知spark，rdd3不用删掉，保留
    # 执行程序后，rdd1,2,4及rdd4往后都会不存在，rdd还存在
    #
    #算子--缓存API：1）rdd3.cache()--将rdd3缓存到内存中 2）rdd3.persist()

    rdd1 = sc.textFile(get_absolute_path("../data/input/words.txt"))
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))

    # 自动缓存API，缓存后，计算从绿色的小点点开始的，绿点之前的东西没有再执行了
    # 底层是分散在多个node上的内存或者硬盘上：分散存储

    rdd3.cache() # 将rdd3缓存到内存中，但是内存缓存多了，则计算需要的内存可能不够，不一定都要缓存到内存，可以放硬盘--内存缓存不安全
    # 手动设置缓存级别的API
    rdd3.persist(StorageLevel.MEMORY_AND_DISK_2) # 内存和磁盘2副本，先存内存，内存不行了再硬盘

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    # 一个action带来一个任务job--对rdd1，2，3，4的链条迭代。3个转换操作有3个节点
    print(rdd4.collect()) # 如果没有rdd3.cache()，print(rdd4.collect())代码一但执行到，rdd4,rdd3,rdd2,rdd1就都不存在了

    rdd5 = rdd3.groupByKey() # rdd3被重复使用
    rdd6 = rdd5.mapValues(lambda x: sum(x))  # rdd5也被重复使用
    # 一个action带来一个任务job
    print(rdd6.collect())
    # 通过node1:4040端口的的第2个job的DAG Visualization看出，我们以为是从rdd3的groupByKey()开始的，从节点看出实际上是从头来的
    # 都是重新从textFile开始的。

    # 总共两个action--所以本程序有两个job任务--对rdd3，5，6的链条进行迭代计算

    rdd3.unpersist() # 主动清理缓存的API--主动释放rdd3
    time.sleep(100000)
    sc.stop()