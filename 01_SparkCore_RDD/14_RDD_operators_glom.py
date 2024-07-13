# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：intersection交集
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

# glom--将RDD的数据，加上嵌套，这个嵌套按照分区来进行的
# 比如RDD数据[1,2,3,4,5] 有两个分区
# 那么，被glom后，数据变成[[1,2,3],[4,5]]
# 作用：一目了然的看出谁和谁在哪个分区

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 2)

    print(rdd.collect())
    # 显示分区嵌套
    print(rdd.glom().collect())
    # 想把结果集合解开嵌套
    # 可以使用map+解嵌套，但是map多余。我只想要解嵌套不想进行map处理--使用flatmap 传入空实现
    # 传参数进去，啥也不做的,原样返回
    print(rdd.glom().flatMap(lambda x: x).collect())
    sc.stop()
