# usual imports
import os
import sys
import time
import glob
import string
import datetime
import numpy as np

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
pool = ConnectionPool('msd_01', ['169.53.141.8:9160'])
cf = ColumnFamily(pool, 'songs')

msd_subset_path='/mnt/million-songs'
msd_subset_data_path=os.path.join(msd_subset_path,'data')
#msd_subset_addf_path=os.path.join(msd_subset_path,'AdditionalFiles')
assert os.path.isdir(msd_subset_path),'wrong path'

msd_code_path='/mnt/MSongsDB'
assert os.path.isdir(msd_code_path),'wrong path'

# we add some paths to python so we can import MSD code
# Ubuntu: you can change the environment variable PYTHONPATH
# in your .bashrc file so you do not have to type these lines
sys.path.append(os.path.join(msd_code_path,'PythonSrc'))

# imports specific to the MSD
import hdf5_getters as GETTERS


cnt = 0
loops = 0


for alpha in string.ascii_uppercase :
   for root, dirs, files in os.walk('/mnt/million-songs/data/'+alpha):
      files = glob.glob(os.path.join(root,'*'+'.h5'))
      for f in files :
         h5 = GETTERS.open_h5_file_read(f)
         num_songs = GETTERS.get_num_songs(h5)
         print f, num_songs

         for i in range(num_songs):
            analysis_sample_rate = GETTERS.get_analysis_sample_rate(h5, i)
            artist_7digitalid = GETTERS.get_artist_7digitalid(h5, i)
            artist_familiarity = GETTERS.get_artist_familiarity(h5, i)
            artist_hotttnesss = GETTERS.get_artist_hotttnesss(h5, i)
            artist_id = GETTERS.get_artist_id(h5, i)
            artist_latitude = GETTERS.get_artist_latitude(h5, i)
            artist_location = GETTERS.get_artist_location(h5, i)
            artist_longitude = GETTERS.get_artist_longitude(h5, i)
            artist_mbid = GETTERS.get_artist_mbid(h5, i)
            artist_mbtags = ','.join(str(e) for e in GETTERS.get_artist_mbtags(h5, i)) # array
            artist_mbtags_count = ','.join(str(e) for e in GETTERS.get_artist_mbtags_count(h5, i)) # array
            artist_name = GETTERS.get_artist_name(h5, i)
            artist_playmeid = GETTERS.get_artist_playmeid(h5, i)
            artist_terms = ','.join(str(e) for e in GETTERS.get_artist_terms(h5, i)) # array
            #artist_terms_freq = ','.join(str(e) for e in GETTERS.get_artist_terms_freq(h5, i)) # array
            #artist_terms_weight = ','.join(str(e) for e in GETTERS.get_artist_terms_weight(h5, i)) # array
            #audio_md5 = GETTERS.get_audio_md5(h5, i)
            #bars_confidence = ','.join(str(e) for e in GETTERS.get_bars_confidence(h5, i)) # array
            #bars_start = ','.join(str(e) for e in GETTERS.get_bars_start(h5, i)) # array
            #beats_confidence = ','.join(str(e) for e in GETTERS.get_beats_confidence(h5, i)) # array
            #beats_start = ','.join(str(e) for e in GETTERS.get_beats_start(h5, i)) # array
            danceability = GETTERS.get_danceability(h5, i)
            duration = GETTERS.get_duration(h5, i)
            end_of_fade_in = GETTERS.get_end_of_fade_in(h5, i)
            energy = GETTERS.get_energy(h5, i)
            key = GETTERS.get_key(h5, i)
            key_confidence = GETTERS.get_key_confidence(h5, i)
            loudness = GETTERS.get_loudness(h5, i)
            mode = GETTERS.get_mode(h5, i)
            mode_confidence = GETTERS.get_mode_confidence(h5, i)
            release = GETTERS.get_release(h5, i)
            release_7digitalid = GETTERS.get_release_7digitalid(h5, i)
            #sections_confidence = ','.join(str(e) for e in GETTERS.get_sections_confidence(h5, i)) # array
            #sections_start = ','.join(str(e) for e in GETTERS.get_sections_start(h5, i)) # array
            #segments_confidence = ','.join(str(e) for e in GETTERS.get_segments_confidence(h5, i)) # array
            #segments_loudness_max = ','.join(str(e) for e in GETTERS.get_segments_loudness_max(h5, i)) # array
            #segments_loudness_max_time = ','.join(str(e) for e in GETTERS.get_segments_loudness_max_time(h5, i)) # array
            #segments_loudness_start = ','.join(str(e) for e in GETTERS.get_segments_loudness_start(h5, i)) # array
            #segments_pitches = ','.join(str(e) for e in GETTERS.get_segments_pitches(h5, i)) # array
            #segments_start = ','.join(str(e) for e in GETTERS.get_segments_start(h5, i)) # array
            #segments_timbre = ','.join(str(e) for e in GETTERS.get_segments_timbre(h5, i)) # array
            similar_artists = ','.join(str(e) for e in GETTERS.get_similar_artists(h5, i)) # array
            song_hotttnesss = GETTERS.get_song_hotttnesss(h5, i)
            song_id = GETTERS.get_song_id(h5, i)
            start_of_fade_out = GETTERS.get_start_of_fade_out(h5, i)
            #tatums_confidence = ','.join(str(e) for e in GETTERS.get_tatums_confidence(h5, i)) # array
            #tatums_start = ','.join(str(e) for e in GETTERS.get_tatums_start(h5, i)) # array
            tempo = GETTERS.get_tempo(h5, i)
            time_signature = GETTERS.get_time_signature(h5, i)
            time_signature_confidence = GETTERS.get_time_signature_confidence(h5, i)
            title = GETTERS.get_title(h5, i)
            track_7digitalid = GETTERS.get_track_7digitalid(h5, i)
            track_id = GETTERS.get_track_id(h5, i)
            year = GETTERS.get_year(h5, i)
            loops += 1




            #row = {'analysis_sample_rate':analysis_sample_rate,'artist_7digitalid':artist_7digitalid,'artist_familiarity':artist_familiarity,'artist_hotttnesss':artist_hotttnesss,'artist_id':artist_id,'artist_latitude':artist_latitude,'artist_location':artist_location,'artist_longitude':artist_longitude,'artist_mbid':artist_mbid,'artist_mbtags_count':artist_mbtags_count,'artist_mbtags':artist_mbtags,'artist_name':artist_name,'artist_terms_freq':artist_terms_freq,'artist_terms_weight':artist_terms_weight,'artist_terms':artist_terms,'audio_md5':audio_md5,'bars_confidence':bars_confidence,'bars_start':bars_start,'beats_confidence':beats_confidence,'beats_start':beats_start,'danceability':danceability,'duration':duration,'end_of_fade_in':end_of_fade_in,'energy':energy,'key_confidence':key_confidence,'key':key,'loudness':loudness,'mode_confidence':mode_confidence,'mode':mode,'release_7digitalid':release_7digitalid,'release':release,'sections_confidence':sections_confidence,'sections_start':sections_start,'segments_confidence':segments_confidence,'segments_loudness_max_time':segments_loudness_max_time,'segments_loudness_max':segments_loudness_max,'segments_loudness_start':segments_loudness_start,'segments_pitches':segments_pitches,'segments_start':segments_start,'segments_timbre':segments_timbre,'similar_artists':similar_artists,'song_hotttnesss':song_hotttnesss,'song_id':song_id,'start_of_fade_out':start_of_fade_out,'tatums_confidence':tatums_confidence,'tatums_start':tatums_start,'tempo':tempo,'time_signature_confidence':time_signature_confidence,'time_signature':time_signature,'title':title,'track_7digitalid':track_7digitalid,'track_id':track_id,'year':year,}
            row = {'analysis_sample_rate':analysis_sample_rate,'artist_7digitalid':artist_7digitalid,'artist_familiarity':artist_familiarity,'artist_hotttnesss':artist_hotttnesss,'artist_id':artist_id,'artist_latitude':artist_latitude,'artist_location':artist_location,'artist_longitude':artist_longitude,'artist_mbid':artist_mbid,'artist_mbtags_count':artist_mbtags_count,'artist_mbtags':artist_mbtags,'artist_name':artist_name,'artist_terms':artist_terms,'danceability':danceability,'duration':duration,'end_of_fade_in':end_of_fade_in,'energy':energy,'key_confidence':key_confidence,'key':key,'loudness':loudness,'mode_confidence':mode_confidence,'mode':mode,'release_7digitalid':release_7digitalid,'release':release,'similar_artists':similar_artists,'song_hotttnesss':song_hotttnesss,'song_id':song_id,'start_of_fade_out':start_of_fade_out,'tempo':tempo,'time_signature_confidence':time_signature_confidence,'time_signature':time_signature,'title':title,'track_7digitalid':track_7digitalid,'track_id':track_id,'year':year,
            }
            key = (row['track_id'])

            #for k, v in row.items(): print k, type(v)
            cf.insert(key,row)
            
            #print "Artist: %s\nSong: %s\nDuration: %s\nYear: %s\nHotness: %s\nLoudness: %s\nDanceability:%s\n\n" % (artist_name, title, duration, year, song_hotttnesss, loudness, danceability)

         h5.close()


   # count files
   cnt += len(files)

print 'Number of files: %s' % cnt
print 'Number of insert: %s' % loops