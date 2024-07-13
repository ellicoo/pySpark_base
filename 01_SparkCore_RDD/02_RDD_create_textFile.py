
# 导入Spark的相关包
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：通过textFile创建rdd
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
    # 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 通过textFile API 读取数据

    # 读取本地文件数据---将文件数据放入RDD中：RDD中每个元素就是文件的一行
    file_rdd1 = sc.textFile(get_absolute_path("../data/input/words.txt"))
    print("默认读取分区数: ", file_rdd1.getNumPartitions())
    print("file_rdd1 内容:", file_rdd1.collect())

    # 加最小分区数参数的测试
    file_rdd2 = sc.textFile(get_absolute_path("../data/input/words.txt"), 3)
    # 最小分区数是参考值, Spark有自己的判断, 你给的太大Spark不会理会
    file_rdd3 = sc.textFile(get_absolute_path("../data/input/words.txt"), 100)
    print("file_rdd2 分区数:", file_rdd2.getNumPartitions())
    print("file_rdd3 分区数:", file_rdd3.getNumPartitions())

    # 读取HDFS文件数据测试
    hdfs_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")
    print("hdfs_rdd 内容:", hdfs_rdd.collect())

    # 5.关闭SparkContext
    sc.stop()