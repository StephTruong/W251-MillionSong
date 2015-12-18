# Clustering
## Directory:
code/summaryh5_to_csv.py script to convert the compressed summary h5 file containing almost all the songs to a text file which is then manually stored in HDFS

code/final_km.py pyspark script that performs Kmeans clustering on the MSD tracks( hardcoded the file location containing track information created in summaryh5_to_csv.py). The script also perform additional sql querying to get aggrrgate statistics about the cluster distributions

code/final_km_lyrics.py same as above except includes sentiment analysis information joined with the MSD track data.

code/predict_tags.py script to perform Logistic Regression in order to predict the genre label given a song's cluster label and lyric sentiment score.

graphs_and_statistics folder contain various plots and summary statistics on the k means clusters of songs and predicted genre labels.