# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：distinct用法
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

    rdd1 = sc.parallelize([ (1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoliu") ])
    rdd2 = sc.parallelize([ (1001, "销售部"), (1002, "科技部")])

    # 通过join算子来进行rdd之间的关联
    # 对于join算子来说 关联条件 按照二元元组的key来进行关联
    # 将关联的rdd元素的键值对中的值放在一个元祖中
    print(rdd1.join(rdd2).collect())

    # 左外连接, 右外连接 可以更换一下rdd的顺序 或者调用rightOuterJoin即可
    print(rdd1.leftOuterJoin(rdd2).collect())

    # 5.关闭SparkContext
    sc.stop()
