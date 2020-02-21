# wordcount 统计示例


from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf()\
        .setMaster("local")\
        .setAppName("wordcount")
    sc = SparkContext(conf=conf)

    sc.textFile("src/main/resources/data/words")\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda v1, v2: v1 + v2)\
        .sortBy(lambda elem: elem[1], ascending=False)\
        .foreach(print)

    sc.stop()
