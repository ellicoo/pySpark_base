# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：
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

    rdd1 = sc.parallelize([1, 3, 5, 3, 1, 3, 2, 6, 7, 8, 6], 1)
    rdd2 = sc.parallelize(['a', 'b', 'c', 'd', 'e', 'r'], 1)
    rdd3 = sc.parallelize(['a', 'b', 'c', 'd', 'e', 'r', 1, 2, 3, 4, 5, 6], 1)

    # first()--action算子--取出当前rdd中的第一个元素---action算子，吧rdd转成非rdd类型
    print('-------first()-----------')
    print(rdd1.first())
    print(rdd2.first())
    print(rdd3.first())
    print('-------take()-----------')
    # take()--action算子--返回前n个元素----list列表形式展示
    print(rdd1.take(1))
    print(rdd2.take(2))
    print(rdd3.take(3))
    print('-------top()-----------')

    # top()--action算子--将rdd的结果集进行降序。提取前n个元素
    print(rdd1.top(4))

    # count()--返回rdd集合的元素个数
    print('--------count()---------')
    print(rdd1.count())
    print('-----------takeSample(参数1, 参数2, 参数3)-------------')
    # takeSample()函数：随机抽样rdd的数据
    # 参数1：True--运行取同一个数据，False--不允许取同一个数据，和数据内容无关，是否重复表示的是
    #       同一个位置的数据
    # 参数2：抽样要几个
    # 参数3：（可以不用传）随机数种子，这个参数传入一个数字即可，随意给,一般第三个参数可以不用传，spark自动给我出
    print(rdd1.takeSample(False, 5, 1)) # 给定种子后，如果种子1不产生变化，那么取出来的位置都不变，则结果不变
    print(rdd1.takeSample(True,22))
    print(rdd1.takeSample(False,22)) # 不允许重复(位置的重复，不是值的重复)时，打不出22个
    sc.stop()
