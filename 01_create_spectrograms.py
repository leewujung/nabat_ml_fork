# This notebook creates spectrogram images from a directory of sound files in .wav format.
# The code is optimized to be processed in parallel on multi-core machines.

import gc
import glob
import multiprocessing as mp
import pprint
from multiprocessing import Pool

from db import NABat_DB

import db_handler
# Test we have a valid database and enumerate the species represented.
db = NABat_DB(p="db_test_20230911")
species = db.query(' select * from species;')
pprint.pprint(species)
db.conn.close()
assert len(species) == 53

# Point to a directory containing .wav files organized by species code. 
# Example "../v1.1.0/data/wav/ANPA/p163_g89522_f28390444.wav"
# directory = '../Downloads/data/wav'
directory = "/Users/Shared/NABat/data/wav"

# Use as many threads as we can, leaving one available to keep notebook responsive.
thread_count = (mp.cpu_count() - 2)
print('using {} threads'.format(thread_count))
 
# Gather wav files.
files = glob.glob('{}/**/*.wav'.format(directory), recursive=True)
progress = int(len(files) * 0.01)

# Verify total number of files
assert len(files) == 21586
assert progress == 215


# Start the creation process in parallel and report progress.
print("Start processing all wav files...")
for i in range(0,len(files),progress):
    with Pool(thread_count) as p:
        p.map(db_handler.process_file, files[i:i+progress])
        gc.collect()
        print('{}%'.format(int(i/progress)))

# Done!
print("Processing done!")
