# coding:utf8
import os

from pyhive import hive
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'
if __name__ == '__main__':
    # 获取到Hive(Spark ThriftServer的链接)
    conn = hive.Connection(host="node1", port=10000, username="hadoop")

    # 获取一个游标对象
    cursor = conn.cursor()

    # 执行SQL
    cursor.execute("SELECT * FROM student")

    # 通过fetchall API 获得返回值
    result = cursor.fetchall()

    print(result)
