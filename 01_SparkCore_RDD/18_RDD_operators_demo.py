# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext
import json

"""
-------------------------------------------------
   Description :	TODO：sortByKey
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

# 订单数据往往以json格式存储
# 需求：读取data文件夹中的order.text文件，提取北京的数据，组合北京和商品类别进行输出
# 同时对结果集进行去重，得到北京售卖的商品类别信息


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取数据文件
    file_rdd = sc.textFile(get_absolute_path("../data/input/order.text"))

    # 一行数据有多个json，需要分割
    # 进行rdd数据的split 按照|符号进行, 得到一个个的json数据
    jsons_rdd = file_rdd.flatMap(lambda line: line.split("|"))

    # 通过Python 内置的json库, 完成json字符串到字典对象的转换
    # 匿名函数传入参数json_str，再处理参数json_str--json.loads(json_str)
    dict_rdd = jsons_rdd.map(lambda json_str: json.loads(json_str))

    # 过滤数据, 只保留北京的数据
    beijing_rdd = dict_rdd.filter(lambda d: d['areaName'] == "北京")

    # 组合北京 和 商品类型形成新的字符串
    category_rdd = beijing_rdd.map(lambda x: x['areaName'] + "_" + x['category'])

    # 对结果集进行去重操作
    result_rdd = category_rdd.distinct()

    # 输出
    print(result_rdd.collect())
    sc.stop()