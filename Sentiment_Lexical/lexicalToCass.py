import csv
from cassandra.cluster import Cluster

# Write the lexical data into cassandra along with the genre

cluster = Cluster(['169.53.141.8'])
session = cluster.connect('msd_01')

# Setup strings for matching genres
pop_str = 'pop'
dance_str = 'dance'
electro_str = 'electronic'
rock_str = 'rock'
alt_str = 'alternative'
rnb_str = 'r&b'
rap_str = 'rap'
hiphop_str = 'hip hop'
hiphop2_str = 'hip-hop'
country_str = 'country'
gospel_str = 'gospel'
contemporary_str = 'contemporary'
funk_str = 'funk'
soul_str = 'soul'
indie_str = 'indie'
metal_str = 'metal'

# Open the csv file with tracks, unique words and lexical diversity
with open('lexical.csv', 'rb') as csvfile:
   reader = csv.reader(csvfile, delimiter=',')
   
   for csvrow in reader:
      track_id = csvrow[0]
      unique_words = csvrow[1]
      lexical_diversity = csvrow[2]

      select_query = "select title, artist_name, artist_terms from songs where track_id='%s'" % (track_id)
      cassrows = session.execute(select_query)

      for cassrow in cassrows:
         title = cassrow.title
         artist = cassrow.artist_name
         terms = cassrow.artist_terms

         # Initialize genre flags for songs
         pop_flg, edm_flg, rock_flg, alt_flg, rnb_flg, rap_flg, country_flg, gospel_flg, funk_flg, indie_flg, metal_flg = False, False, False, False, False, False, False, False, False, False, False

         # Search through the terms for matches against genres
         # Pop
         if pop_str in terms:
            pop_flg = True
      
         # Dance/Electronic
         if dance_str in terms or electro_str in terms:
            edm_flg = True

         # Rock
         if rock_str in terms:
            rock_flg = True

         # Alternative
         if alt_str in terms:
            alt_flg = True

         # R&B
         if rnb_str in terms:
            rnb_flg = True

         # Rap
         if rap_str in terms or hiphop_str in terms or hiphop2_str in terms:
            rap_flg = True

         # Country
         if country_str in terms:
            country_flg = True

         # Gospel/Contemporary
         if gospel_str in terms or contemporary_str in terms:
            gospel_flg = True

         # Funk/Soul
         if funk_str in terms or soul_str in terms:
            funk_flg = True

         # Indie
         if indie_str in terms:
            indie_flg = True

         # Metal
         if metal_str in terms:
            metal_flg = True

         # Write a query to update columns in the database
         update_query = """update songs set
                        unique_words = %s,
                        lexical_diversity = %s,
                        pop = %s,
                        edm = %s,
                        rock = %s,
                        alternative = %s,
                        rhythm_blues = %s,
                        rap = %s,
                        country = %s,
                        gospel = %s,
                        funk_soul = %s,
                        indie = %s,
                        metal = %s
                        where track_id = '%s'""" % (unique_words, lexical_diversity, pop_flg, edm_flg, rock_flg, alt_flg, rnb_flg, rap_flg, country_flg, gospel_flg, funk_flg, indie_flg, metal_flg, track_id)


         session.execute(update_query)

cluster.shutdown()
