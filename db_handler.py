# imports

# With the exception of the librosa library installed above, all of these modules are 
# either included in the code base or provided by default on Amazon Sagemaker. 

import random
from pathlib import Path
import exiftool

from db import NABat_DB
from spectrogram import Spectrogram
    
SPECTROGRAM_LOCATION = "/Volumes/sd4tb_1/NABat/data/images"

"""
This submodule was created to call process_file in 01_create_spectrograms.ipynb with multiprocessing.Pool

The previous version of methods in the notebook would cause AttributeError. Solution inspired by StackOverflow:
https://stackoverflow.com/questions/41385708/multiprocessing-example-giving-attributeerror
"""

# Given a species code, return a numeric id.
def get_manual_id(species_code, species_code_all):
    for s in species_code_all:
        if s.species_code == species_code:
            return s.id


def get_grts_id(file_path):

    grts_ID = (Path(file_path).name).split('-')[-1].split('.')[0]
    with exiftool.ExifToolHelper() as et:
        metadata = et.get_metadata(file_path)
        d = metadata[0]
        if 'RIFF:Guano' in d.keys():
            if 'Grts Id' in d['RIFF:Guano']:
                before_ID = d['RIFF:Guano'].split('Grts Id: ')[1]
                grts_ID = before_ID.split('\n')[0]

    return grts_ID


# This method is meant to be called in parallel and will take a single file path
# and produce a spectrogram for each pulse detected using a BLED within the recording.
def process_file(file, db_name="db_test_20230911"):
    # Randomly and proprotionally assign files to the train, validate, and test sets.
    # 80% train, 10% validate, 10% test
    draw = None
    r = random.random()
    if r < 0.80:
        draw = 'train'
    elif r < 0.90:
        draw = 'test'
    else:
        draw = 'validate'
      

    db = NABat_DB(p=db_name)
    species_code_all = db.query('select * from species;')
    
    # Get metadata about the recording from the file name.
    species_code = file.split('/')[-2]
    manual_id = get_manual_id(species_code, species_code_all)
    grts_id = get_grts_id(file)
    file_name = Path(file).stem

    file_path = Path('{}/{}/{}'.format(SPECTROGRAM_LOCATION, species_code, file_name))
    file_path.mkdir(parents=True, exist_ok=True)

    # Process file and return pulse metadata.
    spectrogram = Spectrogram()
    d = spectrogram.create_from_file(file)

    # Add the file to the database.
    file_id, draw = db.add_file(
                    file_name, d.duration, d.sample_rate, manual_id, grts_id, draw=draw)

    # For each pulse within file...
    for i, m in enumerate(d.metadata):
        # ...create a place to put the spectrogram.
        path = '{}/{}/{}/t_{}.png'.format(SPECTROGRAM_LOCATION, species_code, file_name, m.offset)
        
        # Add the pulse to the database.
        pulse_id = db.add_pulse(file_id, m.frequency,
                                m.amplitude, m.snr, m.offset, m.time, None, path)
        # On success...
        if pulse_id:
            # ...create a spectrogram image surrounding the pulse and save to disk.
            # If the image already exists on disk, skip this process because very time-consuming to save.
            if (not(Path(path).is_file())):
                img = spectrogram.make_spectrogram(m.window, d.sample_rate)
                img.save(path)
                img.close()
            
    # Close the database connection.
    db.conn.close()