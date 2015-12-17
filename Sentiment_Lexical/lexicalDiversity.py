import sqlite3
import cql
import csv

# Retrieve number of stemmed words for each song and write to CSV

sql_conn = sqlite3.connect('mxm_dataset.db')

sql_res = sql_conn.execute("SELECT count(*), track_id FROM lyrics GROUP BY track_id ORDER BY COUNT")

rowcount = 0
with open('lexical.csv', 'wb') as csvfile:
   csvwriter = csv.writer(csvfile, delimiter=',')
   for sql_row in sql_res:
      if (rowcount % 5000) == 0:
         print rowcount
      ld = float(sql_row[0])/float(5000.0)
      csvwriter.writerow([sql_row[1], sql_row[0], ld])
      rowcount += 1

sql_conn.close()
