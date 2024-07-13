# coding:utf8
import os

from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：map算子练习
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

"""
 Spark Core 模块主要是用 Scala 编写的。Spark Core 包括了 Spark 框架的核心功能和基本组件，
 如分布式数据集（RDD）、任务调度、内存计算、容错性、并行计算等。pyspark想要使用Spark Core 模块的内容，是通过py4j包，
 
 通过python的SparkContext类并通过py4j包来与Scala的 Saprk core 模块进行交互的
 
 Spark Core 是 Spark 的核心模块，它提供了 Spark 的基本功能和核心概念，
 包括分布式数据处理、任务调度、内存计算等。
 
 SparkContext（通常简称为 sc）是 Spark Core 模块中最重要的类之一，
 它是与 Spark 集群通信的入口点，是与 Spark 集群进行交互的主要方式。

class PipelinedRDD(RDD):
class RDD(object):
    # 构造函数略
    # 成员函数
    # （1）将rdd进行持久化存储--事先认为安全，所以不存储血缘关系，只存储一个rdd的计算结果
    def checkpoint(self):
    # （2）底层基本算子
    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        return PipelinedRDD(self, f, preservesPartitioning)
    def mapPartitions(self, f, preservesPartitioning=False):
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    # （3）map算子
    def map(self, f, preservesPartitioning=False):
        return self.mapPartitionsWithIndex(func, preservesPartitioning)
    # （4）flatMap算子
    def flatMap(self, f, preservesPartitioning=False):
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    # （5）getNumPartitions算子
    def getNumPartitions(self):
        返回一个非rdd的值
    # （6）过滤rdd数据集中的元素
    def filter(self, f):
        return self.mapPartitions(func, True)
    # （7）给rdd去重
    def distinct(self, numPartitions=None):
        return self.map(lambda x: (x, None)) \
                   .reduceByKey(lambda x, _: x, numPartitions) \
                   .map(lambda x: x[0])
    # （8）合并rdd--会被SparkContext程序入口类的union方法调用
    def union(self, other):
        return rdd

    # （9）基本分区
    def partitionBy(self, numPartitions, partitionFunc=portable_hash):
        return rdd

    # （10）sortByKey--对rdd根据键值对中的key进行排序
    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
        return self.partitionBy(numPartitions, rangePartitioner).mapPartitions(sortPartition, True)

    # （11）sortBy
    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        return self.keyBy(keyfunc).sortByKey(ascending, numPartitions).values()

    # （12）glom--将RDD的数据，加上嵌套，这个嵌套按照分区来进行的
    def glom(self):
        return self.mapPartitions(func)
    # （13） groupBy
    def groupBy(self, f, numPartitions=None, partitionFunc=portable_hash):
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions, partitionFunc)

    #（14）foreach & foreachPartition
    def foreachPartition(self, f):
        self.mapPartitions(func).count()  # Force evaluation
    def foreach(self, f):
        self.mapPartitions(processPartition).count()
     def count(self):
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()
    def sum(self):
        return self.mapPartitions(lambda x: [sum(x)]).fold(0, operator.add)

    #（15）collect算子--将rdd分布式对象中的每个分区数据，都发送到Driver中，形成一个python list 对象
    def collect(self):
        return list(_load_from_socket(sock_info, self._jrdd_deserializer))
    #（16）groupByKey & reduceByKey & aggregateByKey & foldByKey
    def reduceByKey(self, func, numPartitions=None, partitionFunc=portable_hash):
        return self.combineByKey(lambda x: x, func, func, numPartitions, partitionFunc)

    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None,
                       partitionFunc=portable_hash):
        return self.combineByKey(
            lambda v: seqFunc(createZero(), v), seqFunc, combFunc, numPartitions, partitionFunc)

    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=portable_hash):
        return self.combineByKey(lambda v: func(createZero(), v), func, func, numPartitions,
                                 partitionFunc)
    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None, partitionFunc=portable_hash):
        return shuffled.mapPartitions(_mergeCombiners, preservesPartitioning=True)

    def groupByKey(self, numPartitions=None, partitionFunc=portable_hash)
        return shuffled.mapPartitions(groupByKey, True).mapValues(ResultIterable)

    #（17）缓存和持久化rdd
    def checkpoint(self):
    def cache(self):
    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):


# 初始化SparkContext类的成员变量用
class SparkConf(object):

程序入口类的层级关系：
class SparkContext：
     # （1）构造函数--通过conf对象进行参数的初始化或者直接给定master, appName值
     def __init__(self, master=None, appName=None, conf=None....):

     # （2）成员函数--在Python中，方法（也称为函数）是可以嵌套的
    def parallelize(self, c, numSlices=None):
        # 方法嵌套
        def createRDDServer():
        .....
    # （3）读取文件创建RDD类对象的函数
    def textFile(self, name, minPartitions=None, use_unicode=True):
        return RDD(self._jsc.textFile(name, minPartitions), self,
                   UTF8Deserializer(use_unicode))
    # （4）读取小文件创建RDD类对象的函数
    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        return RDD(self._jsc.wholeTextFiles(path, minPartitions), self,
                   PairDeserializer(UTF8Deserializer(use_unicode), UTF8Deserializer(use_unicode)))

    # （5）合并两个rdd类对象的函数
    def union(self, rdds):
        return RDD(self._jsc.union(jrdds), self, rdds[0]._jrdd_deserializer)

    # （6）将变量设置广播变量的函数--变量名.broadcast--将本地小数据发送给
    def broadcast(self, value):
        Broadcast a read-only variable to the cluster, returning a :class:`Broadcast`
        object for reading it in distributed functions. The variable will
        be sent to each cluster only once.
       return Broadcast(self, value, self._pickled_broadcast_vars)

    # （7）定义分布式累加器函数
    def accumulator(self, value, accum_param=None):
        Create an :class:`Accumulator` with the given initial value, using a given
        :class:`AccumulatorParam` helper object to define how to add values of the
        data type if provided. Default AccumulatorParams are used for integers
        and floating-point numbers if you do not provide one. For other types,
        a custom AccumulatorParam can be used
        
    # （8）设置缓存检查点，也就是缓存rdd到本地文件系统或者hdfs
    def setCheckpointDir(self, dirName):
        Set the directory under which RDDs are going to be checkpointed. The
        directory must be an HDFS path if running on a cluster.
        
    # （9）关闭SparkContext所构建的环境
    def stop(self): 无返回值

SparkContext类对象通常取名sc--通过sc对象使用上面的8个函数 ，第1个函数是初始化sc对象用的





spark的核心是由rdd实现的：即rdd是spark最核心的抽象对象
每个action算子确定一条DAG任务执行链条，每遇到一个action，它就触发它之前的所有操作

一、DAG：有向无环图，执行图
所以：一个action会产生一个job(application中的一个子任务)，每个job有自己的DAG执行图
1个action = 1个DAG = 1个job = 1个application的子任务
如果一整个代码中写了3个action，执行起来就会产生一个application，而application中产生3个job，每个job有各自的DAG

算子：1）转换类算子，2）action算子
区别：
      转换算子返回值100%是rdd，acton算子100%不是rdd
      转换类算子是懒加载的，只有遇到action才会执行，action就是转换类算子处理链条的开关

二、DAG的宽窄依赖和阶段划分：
1）阶段划分：spark会根据DAG，按照宽窄依赖，划分不同的DAG阶段(stage)
 划分依据：从后向前，遇到宽依赖，就划分出一个阶段，称之为stage

2）依赖类型：
窄依赖：父rdd的一个分区，【全部】将数据发给子rdd的一个分区
宽依赖(shuffle依赖)：父rdd的一个分区，将数据发给子rdd的多个分区--简单判断：看父rdd有没有分叉出数据给子rdd的多个分区
宽依赖别名：shuffle依赖
宽依赖通常发生在groupByKey、reduceByKey、join等需要数据重组或混合的操作上。

在stage内部，一定都是：窄依赖(阶段内数据比较规整)

三、内存迭代计算：
spark中task是线程概念，并行的优先级大于内存计算管道PipLine。把所有的计算任务(线程)都放一个executor进程中，固然避免走网络导致的IO压力，
但这样就没有并行属性了。想要全内存计算，直接搞local模式，不需要yarn。

四、spark的并行度：
定义：同一时间内，有多少个task在同时运行
并行度：并行能力的设置，不是设定rdd分区，而是设定并行的数量，因为设置了并行数量，rdd就被构造了一样的分区数量
        先有3个并行度，才有3个分区划分，rdd的一个分区只会被一个task或者说一个并行所处理，一个task可以处理多个rdd(并行的)中的一个分区
        一个executor中有多个task线程

        在多个task线程并行条件下：
        横向看：一个task线程可以处理多个rdd中的一个分区
        竖向看：一个rdd有多个分区，需要被多个task线程处理，而一个task线程只能处理一个rdd中的一个分区

比如设置并行度6，就是要6个task并行在跑，在有6个task并行的前提下，rdd的分区就被划分成6个分区了


如何设置并行度：
（1）代码中设置: 
            conf=SparkConf()
            conf.set("spark.default.parallelism, "100")
（2）客户端设置：bin/spark-submit --conf "spark.default.parallelism = 100"
（3）配置文件中设置： conf/spark-defaults.conf中设置
（4）默认（1，但是不会全部以1来跑，多数时候基于读取文件的分片数量来作为默认并行度）

全局并行度配置的参数：
spark.default.parallelism

全局并行度是推荐设置，不要单独针对rdd改分区，可能会影响内存迭代管道的构建，或者会产生额外的shuffle

五、集群任务中如何规划并行度(只看集群总CPU核心数)
结论：设置为cpu总核心的2～3倍，比如集群可用cpu核心是100个，建议并行度设置为200～1000
100核心如果只设置100并行，某个task比较简单就会先执行完，导致task所在cup核心空闲


七、Driver(领导)的两大组件
（1）DAG调度器：
    工作内容：将逻辑的DAG图进行处理，最终得到逻辑上的Task划分

   【分析】：
    先根据DAG分解task任务，再看给几个executor，推荐100个核心，就给100个executor，一个服务器只开一个executor，如果一个
    服务器开多个executor进程，运行着各自task线程，线程之间的交互走网络(本服务器就是本地环回网络)而不是内存，因为进程隔离，
    一台服务器开一个executor进程即可，再在executor进程中执行多个task线程，这样task线程处理的多个rdd之间的通讯就走内存而不是网络

（2）Task调度器
    工作内容：基于DAG Schedule产生出，来规划这些逻辑的task，应该在哪些物理的executor上运行，以及监控管理他们的运行


简述：一台16核的服务器开1个executor进程，分配16个task线程任务，每次处理(rdd链条链上)多个rdd中的一个分区数据
    总之，只要确保task线程能够榨干cpu就行


总结：spark程序层级关系处理：
1、一个Spark环境（一个local或者yarn环境）可以运行多个application
2、一个代码运行起来，会成为一个application
3、application内部可以有多个job
4、每个job都由rdd的action算子触发，并且每个job有自己的DAG执行图
5、一个job的DAG执行图，会基于宽窄依赖划分成不同的阶段
6、不同阶段内，基于分区数量，形成多个并行的内存迭代管道pipline
7、每个内存迭代管道pipline形成一个task（task是由DAG调度器划分的，将job内划分出具体的task任务，
                                    一个job被划分出来的task在逻辑上称为这个job的taskset）

【注意1】但是spark的性能保证，是在保证并行度的前提下，再尽量去走内存迭代管道pipline。

spark默认受到全局并行度的限制，除了个别算子有特殊分区要求的情况，大部分算子都会遵循全局并行度的要求，来划分自己的分区数
如果全局并行度是3，其实大部分算子分区都是3

【注意2】spark我们一般推荐只设置全局并行度，不要再在算子上设置并行度，改不好就变成shuffle，比如rdd3是3个分区，
        强行改成5个分区后，必然导致父亲rdd2有分叉从而走shuffle，从而产生一个新的阶段，使得rdd3前面的阶段的pipline管道变短，
        计算的内容变少，性能下降
        所以，一改rdd分区，就会影响内存计算管道pipline
        除了一些排序算子外，计算算子就让他默认分区即可即parallelize
        或者textFile时设置的全局分区数，即并行度。

【面试1】：spark是怎么做内存计算的？DAG的作用？stage阶段划分的作用？
【根据DAG先分解任务，再看条件】
1、spark会产生DAG图 -- DAG的作用：就是为了内存计算
2、DAG图基于分区和宽窄依赖关系划分阶段 -- 作用：为了构建内存计算，好从逻辑上构建task任务，变为实际行动，任务规划后再看你实际给几个executor
3、一个阶段的内部都是窄依赖，窄依赖内，如果形成前后的1：1的分区对应关系(rdd不改分区时)，就可以产生许多内存迭代计算的管道
4、这些内存迭代计算的管道，就是一个个具体的执行task-- 1个内存计算管道pipline = 1个task(内部有多个窄依赖的rdd)  = 1个线程
5、一个task是一个具体的线程，任务跑在一个线程内，就是做内存计算了

【面试2】：spark为什么比mapreduce快？
1、spark有丰富的算子，mapreduce只有2个，复杂任务时mapreduce要进行串联，多个mr串联会通过磁盘交互数据 
2、spark是内存迭代，mr是磁盘迭代。spark的rdd算子之间形成DAG基于依赖划分阶段后，在阶段内形成内存迭代管道pipline，暂时不落盘
    但mr的map和reduce之间的交互依旧是通过磁盘来交互的，中间有shuffle(分区、排序、规约、分组)

说人话就是，spark算子多，一个程序搞定，同时rdd算子交互和计算上，可以尽量多的执行内存迭代计算而不是磁盘迭代
但是阶段之间的宽窄依赖，大部分还是网络交互的，因为shuffle

MapReduce执行流程： 
读磁盘(hdfs) -> map(mapTask) -> shuffle ->reduce(reduceTask) ->写磁盘
（1）map：
每个 Map 任务都有一个内存缓冲区(缓冲区大小 100MB )
MapTask任务处理后产生的中间结果：写入内存缓冲区中。如果写人的数据达到内存缓冲的阈值( 80MB )，会启动一个线程将内存中的溢出数据写入磁盘

（2）map和reduce的关系(桥梁)：
Map 阶段处理的数据如何传递给 Reduce 阶段 ？：靠shuffle
Shuffle 会将 MapTask 输出的处理结果数据分发给 ReduceTask ，并在分发的过程中，对数据按 key 进行分区和排序。

（3）reduce：
ReduceTask 的数据流是<key, {value list}>形式，用户可以自定义 reduce()方法进行逻辑处理，最终以<key, value>的形式输出。
MapReduce 框架会自动把 ReduceTask 生成的<key, value>传入 OutputFormat 的 write 方法，实现文件的写入操作。

计算复杂任务时：【读磁盘(hdfs) -> map(mapTask) -> shuffle ->reduce(reduceTask) ->写磁盘】--（磁盘IO）--> 【读磁盘(hdfs) -> map(mapTask) -> shuffle ->reduce(reduceTask) ->写磁盘】



"""


if __name__ == '__main__':
    conf = SparkConf().setAppName("WordCountHelloWorld").setMaster("local[2]")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 需求 : wordcount单词计数, 读取HDFS上的words.txt文件, 对其内部的单词统计出现 的数量
    # 读取文件
    # node1的hdfs的web界面就是：node1:9870
    file_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")

    # 将单词进行切割, 得到一个存储全部单词的集合对象
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    # 将单词转换为元组对象, key是单词, value是数字1
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))

    # 将元组的value 按照key来分组, 对所有的value执行聚合操作(相加)
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 通过collect方法收集RDD的数据打印输出结果
    print(result_rdd.collect())

    # 5.关闭SparkContext
    sc.stop()
