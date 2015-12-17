import csv
from cassandra.cluster import Cluster

# Write sentiment score to Cassandra

cluster = Cluster(['169.53.141.8'])
session = cluster.connect('msd_01')

# Open the csv file with tracks, sentiment score
with open('sentiment.csv', 'rb') as csvfile:
   reader = csv.reader(csvfile, delimiter=',')

   numRows = 0
   
   for csvrow in reader:
      if numRows%5000==0:
         print numRows

      track_id = csvrow[0]
      sentiment = csvrow[1]

      # Write a query to update columns in the database
      update_query = """update songs set
                     sentiment = %s
                     where track_id = '%s'""" % (sentiment, track_id)

      session.execute(update_query)

      numRows += 1

cluster.shutdown()
