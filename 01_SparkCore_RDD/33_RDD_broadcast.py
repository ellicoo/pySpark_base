# coding:utf8
import os
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

"""
-------------------------------------------------
   Description :	TODO：共享变量之广播变量
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


    # 定义元祖列表
    stu_info_list = [(1, '张大仙', 11),
                     (2, '王晓晓', 13),
                     (3, '张甜甜', 11),
                     (4, '王大力', 11)]
    # 1. 将本地Python List对象标记为广播变量--因为本地集合存在driver中，故要给executor需要广播出去
    broadcast = sc.broadcast(stu_info_list) # 告诉spark，给每个executor只发一份stu_info_list对象数据
    # 使用广播变量--vlaue = broadcast.vlaue

    # 定义一个rdd，存在各个executor中的各个分区中
    score_info_rdd = sc.parallelize([
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '编程', 99),
        (3, '语文', 99),
        (4, '英语', 99),
        (1, '语文', 99),
        (3, '英语', 99),
        (2, '编程', 99)
    ])

    def map_func(data):
        id = data[0]
        name = ""
        # 匹配本地list和分布式rdd中的学生ID  匹配成功后 即可获得当前学生的姓名
        # 2. 在使用到本地集合对象的地方, 从广播变量中取出来用即可

        # 使用广播变量broadcast.value
        for stu_info in broadcast.value:
            stu_id = stu_info[0]
            if id == stu_id:
                name = stu_info[1]

        return (name, data[1], data[2])


    # 引出问题：
    # 每个executor进程中的多个分区都有作用的map函数，此时map_func函数在多个分区被分布式执行
    # 每个分区都有map，则每个分区都执行map_func中stu_info_list对象，分区执行运算是线程级别的
    # 如果没有广播变量时，几个分区就需要几个stu_info_list对象
    # 因为executor是进程--进程之内数据共享，即它的内存空间被它下面的各个线程共享，即如果executor中有2个分区，
    # 那么在一个进程内有两份stu_info_list对象数据，内存有点浪费了，可是进程内的线程是共享的，它完全不需要两份stu_info_list对象数据
    # 只需要一份。故只需要给executor一份stu_info_list对象数据即可，网络IO的次数可以减半
    # 解决方法--广播变量

    print(score_info_rdd.map(map_func).collect())
    sc.stop()

"""
场景: 本地集合对象(单机对象) 和 分布式集合对象(RDD) 进行关联的时候
需要将本地集合对象 封装为广播变量
可以节省:
1. 网络IO的次数
2. Executor的内存占用

为啥不都变成rdd，rdd对rdd。这样就不用广播变量了--但是性能下降：因为分区对分区，进行shuffle--多IO
比如rdd1的分区1的数据要给rdd2分区1、2、3的.rdd1的分区2要给rdd2的1，2，3。此时进行的是shutfle
而广播变量给的不是分片数据，给rdd2的是全量的数据，就不需要跟其他分区进行交互，避免大量网络IO

所以使用本地集合的广播变量可以提高一点点性能，但是如果本地集合过大比如10GB，就需要转RDD了，因为driver内存放不下了
"""