from __future__ import print_function # print not included in python 2.6
from pyspark import SparkContext
import pyspark
#import sys
#sys.path.append('/mnt/MSongsDB/PythonSrc/') #this is where hdf5_getters is placed
#import hdf5_getters
from pyspark.mllib.clustering import KMeans
from numpy import array
from pyspark.sql import SQLContext, Row
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy.random as rnd
from scipy import linalg
import numpy as np
from math import sqrt
import pandas as pd

# Prereq: Need to run listfiles.sh to get filelists.txt and insert in Tachyon
# filelists.txt contains all absolute paths to h5 data files


# For each file extract the 12 features and return an array [track_id, f1, f2, f3, f4, f5]+meta
def extract_features(row):
    
    fam = row[9]
    if fam == '':
        fam = 0.0
    
    hot = row[10]
    if hot == '':
        hot = 0.0

    sent = row[11].strip()
    if sent == '':
        sent = 0.0
    
    
    #track_id, tempo , loud , time_sig , duration , key , song title, artist_name, year, artist_familiarity, hotness, sentiment score
    return array([row[0],float(row[1]),float(row[2]),float(row[3]),float(row[4]),float(row[5]),row[6],row[7],float(row[8]),float(fam),float(hot),float(sent)])



def flatten_cluster_data(r):
    # In Key: file_path, song, year, familiarity,hotness); Value= [f1...,f5]
    return {"file_path": str(r[0][0].encode('utf-8').strip()), "song" : str(r[0][1].encode('utf-8').strip()), "artist" : str(r[0][2].encode('utf-8').strip()), "year" : float(r[0][3]), "artist_familiarity" : float(r[0][4]), "hotness" : float(r[0][5]), "cluster" : int(r[1][0]), "tempo": float(r[1][1][0]), "loud": float(r[1][1][1]), "time_sign": float(r[1][1][2]), "duration": float(r[1][1][3]), "key": float(r[1][1][4]),"sent": float(r[0][6])}

if __name__ == "__main__":
    
    sc = SparkContext(appName="SparkSAM-lyrics")
    #hadoopConf = sc._jsc.hadoopConfiguration()
    #hadoopConf.set("fs.tachyon.impl", "tachyon.hadoop.TFS") #set tachyon
    
    # Get the csv file and extract the rows which contains 11 fields + lyrics sentiment score   /combined/combined_files.csv
    track_ids = sc.textFile("hdfs://169.53.141.8:8020/combined/combined_files.csv",100)
    
    # for each row in the csv, extract 11 features and return an np array of all case class object
    parsedData = track_ids.map(lambda a: a.split(',')).map(extract_features)
    #parsedData.persist(pyspark.StorageLevel.OFF_HEAP) #persist off-heap in tachyon

    k=30
    # Key = tuple(track_file_path,meta data) Value = array of f1-f5
    clustering_input_pairs = parsedData.map(lambda x: ((x[0],x[6],x[7],x[8],x[9],x[10],x[11]),array([float(x[1]), float(x[2]),float(x[3]),float(x[4]),float(x[5])])))
   
    #print(clustering_input_pairs.take(5))

    # Create a separate RDD that just stores array of features- this will be the input
    clustering_input = parsedData.map(lambda x: array([float(x[1]), float(x[2]),float(x[3]),float(x[4]),float(x[5])]))
    #print(clustering_input_pairs.take(5))
    
    # After choosing least error K, train model
    final_clusters = KMeans.train(clustering_input, k)
    
    # for each input pair, get the Values (array of features) and predict the cluster label
    cluster_membership = clustering_input_pairs.mapValues(lambda x: final_clusters.predict(x))
   
    #print("Predict 10 Labels!!!!!!")
    #print(cluster_membership.take(10))
   
    # Join 2 RDDs on their keys ie. (K,V) and (K,W) => (K,(V,W)) and flatten the tuple; create a df
    complete_cluster_data = cluster_membership.join(clustering_input_pairs).map(flatten_cluster_data)
    cluster_df = pd.DataFrame(complete_cluster_data.collect())
    #print("Complete Cluster Data!!")
    #print(complete_cluster_data.take(5))


    # Now plot a subset of the df(20 points/cluster) to avoid having too many points
    # Group by cluster
    grouped = cluster_df.groupby(['cluster'])
    
    '''
    fig,ax = plt.subplots(1,1,figsize=(10,10))
    for name, group in grouped:
        color = np.random.rand(3,)
        ax.plot(group['loud'].tail(20), group['tempo'].tail(20), marker='o', linestyle='', color= color,label=name)
        ax.plot(final_clusters.centers[name][3],final_clusters.centers[name][0], 'x', markerfacecolor='k', markeredgecolor='k',mew=2,ms=15)

    ax.legend()
    plt.xlabel("Loudness(dB)")
    plt.ylabel("Tempo(BPM)")
    plt.title("Plotting K=30 clusters in 2D")
    plt.savefig('graphs/KmeansCluster_vs2.png')
    '''


    # Create an SQL context from existing sc
    sqlContext = SQLContext(sc)
    # Infer the schema, and register the DataFrame as a table.
    schemaClusters = sqlContext.createDataFrame(cluster_df)
    schemaClusters.registerTempTable("clusters") #table name is called clusters


    '''    
    #1) Run SQL query to analyze the cluster distributions
    for i in range(k):
        q = "cluster =" + str(i) + " and hotness > 0.02 and artist_familiarity > 0.02 and year > 1900 "
        out = schemaClusters.filter(q).describe()
        o= out.map(lambda p: str(p))
        with open("graphs/Statistics_Clusters_with_lyrics.txt", "a") as text_file:
                text_file.write(str(o.collect()))

    '''

    #2) Get top 20 postive sentiment score songsfor each cluster_id

    for cluster_id in range(k):
        #get the id and artist for one cluster, order by count
        q= "SELECT cluster, song, sent FROM clusters WHERE cluster= "+str(cluster_id)+ " ORDER BY sent DESC LIMIT 20"
        
        artists_by_group = sqlContext.sql(q)
        
        query_2 = artists_by_group.map(lambda p: "Cluster_id: " + str(p.cluster) + " Song: "+ str(p.song.encode('utf-8')) + " Sent_score: "+ str(p.sent))
        
        with open("graphs/PositiveSongsByCluster.txt", "a") as text_file:
            text_file.write(str(query_2.collect()))
   

    #3) Get top 20 negative sentiment score songs for each cluster_id

    for cluster_id in range(k):
        #get the id and artist for one cluster, order by count
        q= "SELECT cluster, song, sent FROM clusters WHERE cluster= "+str(cluster_id)+ " ORDER BY sent ASC LIMIT 20"
        
        artists_by_group = sqlContext.sql(q)
        
        query_3 = artists_by_group.map(lambda p: "Cluster_id: " + str(p.cluster) + " Song: "+ str(p.song.encode('utf-8')) + " Sent_score: "+ str(p.sent))
        
        with open("graphs/NegativeeSongsByCluster.txt", "a") as text_file:
            text_file.write(str(query_3.collect()))

            

    sc.stop()




