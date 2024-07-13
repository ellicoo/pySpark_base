# coding:utf8

from pyspark import SparkConf, SparkContext
# 引入的文件不可以使用数字打头
from defs_19 import city_with_category
import json
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path


"""
-------------------------------------------------
   Description :	TODO：测试大数据条件下跑yarn集群计算
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""

# 代码中手动设置环境变量
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

os.environ['HADOOP_CONF_DIR'] = "/export/server/hadoop/etc/hadoop"

# 企业一般使用local模式进行开发和测试，代码开发完，如果需要计算的数据量非常大如GB或TB时，要放到集群中跑

if __name__ == '__main__':
    # 提交 到yarn集群, master 设置为yarn
    conf = SparkConf().setAppName("test-yarn-1").setMaster("yarn")
    # 如果提交到集群运行, 除了主代码以外, 还依赖了其它的代码文件
    # 需要设置一个参数, 来告知spark ,还有依赖文件要同步上传到集群中
    # 参数叫做: spark.submit.pyFiles
    # 参数的值可以是 单个.py文件,   也可以是.zip压缩包(有多个依赖文件的时候可以用zip压缩后上传)

    # 将主代码文件和其所依赖的代码defs_19.py文件会被一起同步到集群中，然后被同步的分发到各个executor中
    # '但'企业的环境可能不会和生产环境联通，只能通过工具登陆服务器，把代码上传，再通过spark submit来提交
    conf.set("spark.submit.pyFiles", "defs_19.py")
    sc = SparkContext(conf=conf)

    # 在集群中运行, 我们需要用HDFS路径了. 不能用本地路径
    file_rdd = sc.textFile("hdfs://node1:8020/input/order.text")

    # 进行rdd数据的split 按照|符号进行, 得到一个个的json数据
    jsons_rdd = file_rdd.flatMap(lambda line: line.split("|"))

    # 通过Python 内置的json库, 完成json字符串到字典对象的转换
    dict_rdd = jsons_rdd.map(lambda json_str: json.loads(json_str))

    # 过滤数据, 只保留北京的数据
    beijing_rdd = dict_rdd.filter(lambda d: d['areaName'] == "北京")

    # 组合北京 和 商品类型形成新的字符串--city_with_category独立方法抽取出来
    category_rdd = beijing_rdd.map(city_with_category)

    # 对结果集进行去重操作
    result_rdd = category_rdd.distinct()

    # 输出
    print(result_rdd.collect())
    sc.stop()
