from pyspark import SparkContext, SparkConf
import shutil

conf = SparkConf().setAppName('sequenceFiles').setMaster('local').set("spark.ui.port", "4050")
sc = SparkContext(conf=conf)

rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))

shutil.rmtree("sequence_file")

rdd.saveAsSequenceFile("sequence_file")
print(sorted(sc.sequenceFile("sequence_file").collect()))