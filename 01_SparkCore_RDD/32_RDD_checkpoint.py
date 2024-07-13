# coding:utf8
import os
import time

from pyspark import SparkConf, SparkContext
from my_utils.get_local_file_system_absolute_path import get_absolute_path
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

    # cache和persist设计认为是缓存不太安全的，如果缓存消失，则必须保存rdd前置血缘关系，这才能重新计算找到--保存rdd血缘链条

    # checkpoint--设计上被认为是安全的，故不保存rdd血缘链条

    # 区别：
    # 1.存储位置不同
    # cache：存储在内存中
    # Persist：利用Executor的内存和节点磁盘来存储
    # Checkpoint：利用HDFS存储（不会丢失）

    # 2.生命周期不同
    # cache：随着任务的执行完成而消失
    # Persist：如果程序结束或者遇到了unpersist，内存和磁盘中的缓存会被清理
    # Checkpoint：只要不手动删除HDFS，就一直存在

    # 3.存储内容不同
    # cache：缓存整个RDD，保留RDD的依赖关系--分散存
    # Persist：缓存整个RDD，保留RDD的依赖关系--分散存
    # Checkpoint：存储是RDD的数据，不保留RDD的依赖关系--存在硬盘上，建议存在hdfs中--集中到分区，然后各分区到hdfs中存--集中存
    #
    # 4.性能不同：checkpoint<缓存cache/persist（内存更快）--因为executor是并行执行的，又是内存计算
    #          checkpoint比较慢，因为集中存储，涉及网络io，但存储到HDFS中更安全，因为多副本，缺点比较占内存，比存储硬盘慢
    # 5. 应用场景：
    #     如果rdd的数据比较轻量，使用cache缓存，如果数据比较大，重新计算的成不过高，则选择checkpoint重安全
    # 1. 告知spark, 开启CheckPoint功能
    sc.setCheckpointDir("hdfs://node1:8020/output/ckp")  # 设置checkpoint存储路径

    rdd1 = sc.textFile(get_absolute_path("../data/input/words.txt"))
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))

    # 调用checkpoint API 保存数据即可--分区越多风险越高
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    rdd3.unpersist()
    time.sleep(100000)
    sc.stop()
