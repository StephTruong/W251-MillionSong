from pyspark import SparkConf
from pyspark_cassandra import  CassandraSparkContext
from pyspark.sql import SQLContext,Row
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.feature import StandardScaler
import numpy as np

conf = SparkConf().setAppName("Regression on Song Hotness Analysis").setMaster("spark://muziki:7077")
sc= CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Make Spark less verbose
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def parsePoint(data):
	#return LabeledPoint(data[3],np.append(data[0:3],data[4:]))
	return LabeledPoint(data[0],data[1:])

# store the data from cassandra to a data frame and remove the NA value 
data=sc.cassandraTable("msd_01", "songs").select("song_hotttnesss","loudness","year","sentiment","tempo","unique_words").toDF()

data=data.filter("year>0").na.drop()
print data.count()


# Scale the features with Standard Scaler
data2=data.map(lambda x: [x.song_hotttnesss, x.loudness,x.year, x.sentiment,x.tempo,x.unique_words])#Convert each sql.row to an array
scaler= StandardScaler(withMean=True, withStd=True).fit(data2) #fit a scaler on the every column
scaledData = scaler.transform(data2)# transform our data

# Transform to a labelled vector
parsedData = scaledData.map(parsePoint)

# # Build the model
model = LinearRegressionWithSGD.train(parsedData, iterations=1000,regParam=1.0,regType="l2",intercept=True)

# Evaluate the model on training data
print ("intercept",model.intercept)
print zip(["loudness","year","sentiment","tempo","unique_words"],model.weights)

sc.stop()