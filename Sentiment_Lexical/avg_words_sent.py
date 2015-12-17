import csv
from cassandra.cluster import Cluster

# Produce results for average words and sentiment for each genre

cluster = Cluster(['169.53.141.8'])
session = cluster.connect('msd_01')

# Open the csv file with tracks, unique words and lexical diversity
with open('lexical.csv', 'rb') as csvfile:
   reader = csv.reader(csvfile, delimiter=',')

   numRows = 1

   pop_words, edm_words, rock_words, alt_words, rnb_words, rap_words, country_words, gospel_words, funk_words, indie_words, metal_words = ([] for i in range(11))

   pop_sent, edm_sent, rock_sent, alt_sent, rnb_sent, rap_sent, country_sent, gospel_sent, funk_sent, indie_sent, metal_sent = ([] for i in range(11))
   
   for csvrow in reader:
      if numRows%5000==0:
         print numRows

      track_id = csvrow[0]

      select_query = """select title, artist_name,
                        unique_words,
                        lexical_diversity,
                        sentiment,
                        pop,
                        edm,
                        rock,
                        alternative,
                        rhythm_blues,
                        rap,
                        country,
                        gospel,
                        funk_soul,
                        indie,
                        metal
                        from songs where track_id='%s'""" % (track_id)

      cassrows = session.execute(select_query)

      for cassrow in cassrows:
         if cassrow.pop:
            pop_words.append(cassrow.unique_words)
            pop_sent.append(cassrow.sentiment)
         if cassrow.edm:
            edm_words.append(cassrow.unique_words)
            edm_sent.append(cassrow.sentiment)
         if cassrow.rock:
            rock_words.append(cassrow.unique_words)
            rock_sent.append(cassrow.sentiment)
         if cassrow.alternative:
            alt_words.append(cassrow.unique_words)
            alt_sent.append(cassrow.sentiment)
         if cassrow.rhythm_blues:
            rnb_words.append(cassrow.unique_words)
            rnb_sent.append(cassrow.sentiment)
         if cassrow.rap:
            rap_words.append(cassrow.unique_words)
            rap_sent.append(cassrow.sentiment)
         if cassrow.country:
            country_words.append(cassrow.unique_words)
            country_sent.append(cassrow.sentiment)
         if cassrow.gospel:
            gospel_words.append(cassrow.unique_words)
            gospel_sent.append(cassrow.sentiment)
         if cassrow.funk_soul:
            funk_words.append(cassrow.unique_words)
            funk_sent.append(cassrow.sentiment)
         if cassrow.indie:
            indie_words.append(cassrow.unique_words)
            indie_sent.append(cassrow.sentiment)
         if cassrow.metal:
            metal_words.append(cassrow.unique_words)
            metal_sent.append(cassrow.sentiment)

      numRows += 1

   
   print "Number of pop songs: %s" % (len(pop_words))
   print "Number of edm songs: %s" % (len(edm_words))
   print "Number of rock songs: %s" % (len(rock_words))
   print "Number of alt songs: %s" % (len(alt_words))
   print "Number of rnb songs: %s" % (len(rnb_words))
   print "Number of rap songs: %s" % (len(rap_words))
   print "Number of country songs: %s" % (len(country_words))
   print "Number of gospel songs: %s" % (len(gospel_words))
   print "Number of funk songs: %s" % (len(funk_words))
   print "Number of indie songs: %s" % (len(indie_words))
   print "Number of metal songs: %s" % (len(metal_words))

   avg_words = []
   avg_words.append(reduce(lambda x, y: x + y, pop_words) / len(pop_words))
   avg_words.append(reduce(lambda x, y: x + y, edm_words) / len(edm_words))
   avg_words.append(reduce(lambda x, y: x + y, rock_words) / len(rock_words))
   avg_words.append(reduce(lambda x, y: x + y, alt_words) / len(alt_words))
   avg_words.append(reduce(lambda x, y: x + y, rnb_words) / len(rnb_words))
   avg_words.append(reduce(lambda x, y: x + y, rap_words) / len(rap_words))
   avg_words.append(reduce(lambda x, y: x + y, country_words) / len(country_words))
   avg_words.append(reduce(lambda x, y: x + y, gospel_words) / len(gospel_words))
   avg_words.append(reduce(lambda x, y: x + y, funk_words) / len(funk_words))
   avg_words.append(reduce(lambda x, y: x + y, indie_words) / len(indie_words))
   avg_words.append(reduce(lambda x, y: x + y, metal_words) / len(metal_words))

   avg_sent = []
   avg_sent.append(reduce(lambda x, y: x + y, pop_sent) / len(pop_sent))
   avg_sent.append(reduce(lambda x, y: x + y, edm_sent) / len(edm_sent))
   avg_sent.append(reduce(lambda x, y: x + y, rock_sent) / len(rock_sent))
   avg_sent.append(reduce(lambda x, y: x + y, alt_sent) / len(alt_sent))
   avg_sent.append(reduce(lambda x, y: x + y, rnb_sent) / len(rnb_sent))
   avg_sent.append(reduce(lambda x, y: x + y, rap_sent) / len(rap_sent))
   avg_sent.append(reduce(lambda x, y: x + y, country_sent) / len(country_sent))
   avg_sent.append(reduce(lambda x, y: x + y, gospel_sent) / len(gospel_sent))
   avg_sent.append(reduce(lambda x, y: x + y, funk_sent) / len(funk_sent))
   avg_sent.append(reduce(lambda x, y: x + y, indie_sent) / len(indie_sent))
   avg_sent.append(reduce(lambda x, y: x + y, metal_sent) / len(metal_sent))

   print avg_words
   print avg_sent

cluster.shutdown()
