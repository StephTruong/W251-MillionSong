# Statistical Analysis with Cassandra Database and Spark MLlib

## Create a database

On Spark master Muziki, connect to Cassandra Cluster with cqlsh. Execute command from createCassDB.txt.

## Load HDF5 in Cassandra

Execute: HDF5_DISABLE_VERSION_CHECK=2 nohup python loadh5tocass.py.
The script goes through all HDF5 files stored in HDFS and use CQL command to insert the rows in Cassandra. We did not load all the columns as not all of them are of interest for our regression analysis. The final list of columns is: 
'analysis_sample_rate':analysis_sample_rate,
'artist_7digitalid':artist_7digitalid,
'artist_familiarity':artist_familiarity,
'artist_hotttnesss':artist_hotttnesss,
'artist_id':artist_id,
'artist_latitude':artist_latitude,
'artist_location':artist_location,
'artist_longitude':artist_longitude,
'artist_mbid':artist_mbid,
'artist_mbtags_count':artist_mbtags_count,
'artist_mbtags':artist_mbtags,
'artist_name':artist_name,
'artist_terms':artist_terms,
'danceability':danceability,
'duration':duration,
'end_of_fade_in':end_of_fade_in,
'energy':energy,
'key_confidence':key_confidence,
'key':key,
'loudness':loudness,
'mode_confidence':mode_confidence,
'mode':mode,
'release_7digitalid':release_7digitalid,
'release':release,
'similar_artists':similar_artists,
'song_hotttnesss':song_hotttnesss,
'song_id':song_id,
'start_of_fade_out':start_of_fade_out,
'tempo':tempo,
'time_signature_confidence':time_signature_confidence,
'time_signature':time_signature,
'title':title,
'track_7digitalid':track_7digitalid,
'track_id':track_id,
'year':year
unique_words
sentiment


## Load data from Cassandra and run regression analysis

Execute:

$SPARK_HOME/bin/spark-submit --packages TargetHolding/pyspark-cassandra:0.2.2 --conf spark.cassandra.connection.host=169.53.141.8 --master=spark://muziki:7077 --num-executors 4 --driver-memory 6g --executor-memory 6g songHotnessRegression.py

The script use pyspark-cassandra connector to pull out Cassandra rows into a Pyspark RDD. A transformation in data frame allows us to conveniently clean the data. We also apply a feature scaling on all columns with MLlib Standard Scaler.

We fit a LinearRegressionWithSGD model on the data. For the parameters we used 1000 iterations, L2 regularisation factor of 1.0.

The weights are displayed in the terminal.

## Library used
pyspark-cassandra: https://github.com/TargetHolding/pyspark-cassandra

