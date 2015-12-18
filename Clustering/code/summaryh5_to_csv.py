from numpy import array
import numpy as np
import csv
import sys
sys.path.append('/mnt/MSongsDB/PythonSrc') #this is where hdf5_getters.py is placed
import hdf5_getters

# This script converts the summary H5 files only 300MB to a csv file
# Run only on the Master Node since h5_getters cannot open a remote(ie. HDFS) file



if __name__ == "__main__":

    with open("fields.csv", "wb") as f:
        writer = csv.writer(f) #initialize the csv writer
        
        # for each track in the summary file, get the 11 fields and output to csv
        h5_file = hdf5_getters.open_h5_file_read('msd_summary_file.h5')
        for k in range(1000000):
            print "index!!!: ", k
            id = hdf5_getters.get_track_id(h5_file,k) #get track_id TRA13e39..
            title = hdf5_getters.get_title(h5_file,k) # get song title
            artist_name = hdf5_getters.get_artist_name(h5_file,k)
            year = int(hdf5_getters.get_year(h5_file,k))
            hotness= float(hdf5_getters.get_song_hotttnesss(h5_file,k))
            artist_familiarity = float(hdf5_getters.get_artist_familiarity(h5_file,k))
            f5 = int(hdf5_getters.get_key(h5_file,k)) #get key
            f2 = float(hdf5_getters.get_loudness(h5_file,k)) #get loudness
            f1 = float(hdf5_getters.get_tempo(h5_file,k)) #get tempo
            f4 = int(hdf5_getters.get_duration(h5_file,k)) #get duration
            f3 = float(hdf5_getters.get_time_signature(h5_file,k)) #get time signature
            
            
            # Get rid of missing info and change invalid numbers for meta data
            
            if not artist_name:
                artist_name = "unknown"

            if not artist_familiarity:
                artist_familiarity=0.0

            if not hotness:
                hotness = 0.0

            if not year:
                year = 0


            #if f1 and f2 and f3 and f4 and f5 and id and title are not empty, insert into csv:
            if f1 and f2 and f3 and f4 and f5 and len(id)>0 and len(title)>0:
                a= [id,f1,f2,f3,f4,f5,title,artist_name,year,artist_familiarity,hotness]
                writer.writerow(a)
        
	h5_file.close()





