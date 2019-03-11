"""
This script runs the signal_timestamps function, that gets the duration of each 
recording + all the timestamps of the recoring, and saves the info in the 
database.
"""

import os
from .signal_extraction import signal_timestamps
import sqlite3

if 'HOSTNAME' in os.environ:
    # script runs on server
    STORAGE_DIR = '/storage/all_german_birds/'
    DATABASE_DIR = '/storage/db.sqlite'
else:
    # script runs locally
    STORAGE_DIR = 'storage/all_german_birds/'
    DATABASE_DIR = 'storage/db.sqlite'

# Get a list of files that are downloaded
downloaded_files = os.listdir(STORAGE_DIR)
print('list with downloaded files made')
print(len(downloaded_files))

# Get the recording ID's from the filenames
downloaded_ids = [int(x[:-4]) for x in downloaded_files]

# Get all the recordings that were already processed before
conn = sqlite3.connect(DATABASE_DIR)
print('database loaded')
c = conn.cursor()
c.execute("""select id from recordings where duration IS NOT NULL""")
processed_ids = [i[0] for i in c.fetchall()]
print('list of already processed recordings')
print(len(processed_ids))

# Remove the already processed recordings from the ones we want to process
to_process = [x for x in downloaded_ids if x not in processed_ids]
print('list of files to process')
print(len(to_process))

# Processing
batch = []
for i, rec_id in enumerate(to_process):
    rec = str(rec_id) + '.mp3'
    print(rec_id)
    try:
        duration, sum_signal, timestamps = signal_timestamps(
            STORAGE_DIR + rec)
        batch.append((duration, sum_signal, timestamps, rec_id))
        if len(batch) % 10 == 0:
            print(f"batch {i} full")
            c.executemany("""UPDATE recordings SET duration = ?, sum_signal = ?, timestamps = ? 
                WHERE id = ?""", batch)
            conn.commit()
            batch = []
    except:
        pass

conn.close()
