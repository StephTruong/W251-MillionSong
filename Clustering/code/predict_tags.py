from __future__ import print_function # print not included in python 2.6
from pyspark import SparkContext
import pyspark
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
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

    genre = row[12].strip()
    if genre == '' or genre =='NaN':
        genre = 'unknown'

    #track_id, tempo , loud , time_sig , duration , key , song title, artist_name, year, artist_familiarity, hotness, sentiment score, genre_tag
    return array([row[0],float(row[1]),float(row[2]),float(row[3]),float(row[4]),float(row[5]),row[6],row[7],float(row[8]),float(fam),float(hot),float(sent), genre])



def flatten_cluster_data(r):
    # Create genre mapping to a number
    genre_id = 0
    genre = str(r[0][7])
    
    if genre == 'Pop_Rock':
                  genre_id = 0
    elif genre == 'Electronic':
                  genre_id= 1
    elif genre == 'Rap':
                  genre_id= 2
    elif genre == 'Jazz':
                  genre_id= 3
    elif genre == 'Latin':
                  genre_id= 4
    elif genre == 'International':
                  genre_id= 5
    elif genre == 'Country':
                  genre_id= 6
    elif genre == 'Reggae':
                  genre_id= 7
    elif genre == 'Blues':
                  genre_id= 8
    elif genre == 'Vocal':
                  genre_id= 9
    elif genre == 'Folk':
                  genre_id= 10
    elif genre == 'RnB':
                  genre_id= 11
    else: #New Age
                  genre_id= 12
                  
    # In Key: file_path, song, year, familiarity,hotness); Value= [f1...,f5]
    return {"file_path": str(r[0][0].encode('utf-8').strip()), "song" : str(r[0][1].encode('utf-8').strip()), "artist" : str(r[0][2].encode('utf-8').strip()), "year" : float(r[0][3]), "artist_familiarity" : float(r[0][4]), "hotness" : float(r[0][5]), "genre" : genre_id, "cluster" : int(r[1][0]), "tempo": float(r[1][1][0]), "loud": float(r[1][1][1]), "time_sign": float(r[1][1][2]), "duration": float(r[1][1][3]), "key": float(r[1][1][4]),"sent": float(r[0][6])}

if __name__ == "__main__":
    
    sc = SparkContext(appName="SparkSAM-predict")
    
    # Get the csv file and extract the rows which contains 11 fields + lyrics sentiment score + genre tag
    train_ids = sc.textFile("hdfs://169.53.141.8:8020/tags/train_file.csv",100)
    test_ids = sc.textFile("hdfs://169.53.141.8:8020/tags/test_file.csv",100)
    
    # for each row in the csv, extract 13 features and return an np array of all case class object
    parsedData = train_ids.map(lambda a: a.split(',')).map(extract_features)
    parsedData_test = test_ids.map(lambda a: a.split(',')).map(extract_features)
    
    #Apply Kmeans clustering algorithm first
    k=30
    # Key = tuple(track_file_path,meta data,lyric,tag) Value = array of f1-f5
    clustering_input_pairs = parsedData.map(lambda x: ((x[0],x[6],x[7],x[8],x[9],x[10],x[11],x[12]),array([float(x[1]), float(x[2]),float(x[3]),float(x[4]),float(x[5])])))
                  
    clustering_input_pairs_test = parsedData_test.map(lambda x: ((x[0],x[6],x[7],x[8],x[9],x[10],x[11],x[12]),array([float(x[1]), float(x[2]),float(x[3]),float(x[4]),float(x[5])])))
    
    #print(clustering_input_pairs.take(5))
    
    # Create a separate RDD that just stores array of features- this will be the input
    clustering_input = parsedData.map(lambda x: array([float(x[1]), float(x[2]),float(x[3]),float(x[4]),float(x[5])]))
    clustering_input_test = parsedData_test.map(lambda x: array([float(x[1]), float(x[2]),float(x[3]),float(x[4]),float(x[5])]))
    #print(clustering_input_pairs.take(5))
    
    # After choosing least error K, train model
    final_clusters = KMeans.train(clustering_input, k)
    final_clusters_test = KMeans.train(clustering_input_test, k)
                  
    # for each input pair, get the Values (array of features) and predict the cluster label
    cluster_membership = clustering_input_pairs.mapValues(lambda x: final_clusters.predict(x))
    cluster_membership_test = clustering_input_pairs_test.mapValues(lambda x: final_clusters_test.predict(x))
    #print("Predict 10 Labels!!!!!!")
    #print(cluster_membership.take(10))
    
    # Join 2 RDDs on their keys ie. (K,V) and (K,W) => (K,(V,W)) and flatten the tuple; turns genre into a float
    complete_cluster_data = cluster_membership.join(clustering_input_pairs).map(flatten_cluster_data)
    complete_cluster_data_test = cluster_membership_test.join(clustering_input_pairs_test).map(flatten_cluster_data)
    #   print("CLUSTER DATA!!!")
    # print(complete_cluster_data.take(2))        

  
    # Get each row of complete cluster data and first create number for categorical genre tags
    nb_train = complete_cluster_data.map(lambda x:  LabeledPoint(x['genre'], [x['cluster'],x['sent'] ]))
    # Create a Key= track_id, Values = LabeledPoint
    nb_test = complete_cluster_data_test.map(lambda x: (x['file_path'],LabeledPoint(x['genre'], [x['cluster'],x['sent'] ])))
    #print("NB TEST!!!")
    #print(nb_test.take(2))                  
    nB_model = LogisticRegressionWithLBFGS.train(nb_train,numClasses=13)

    # Make prediction on the model and test accuracy. Predict on the array(cluster,lyrics)
    predictionAndLabel = nb_test.map(lambda p : (p[0],p[1].features, nB_model.predict(p[1].features), p[1].label))
                  
    # Get the number predicted right/total test data points
    accuracy = 1.0 * predictionAndLabel.filter(lambda (i,f, p, a): p == a).count() / nb_test.count()
    print("ACCURACY SCORE!!!!!!!!:")
    print(accuracy)
    
    with open("graphs/predictions_2", "a") as text_file:
                text_file.write(str(predictionAndLabel.collect()))              
                  
    sc.stop()





