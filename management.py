import os
from birdsong.data_preparation.balanced_split import make_split
from Testing_signal_noise import Slicer
from collections import Counter
import pandas as pd
import sqlite3
import sql_selectors

class DatabaseManager(object):
    """ This class bundles the various functions for acquiring, inventorizing, 
    manipulating and serving data and exposes them as easily usable methods."""
    def __init__(self, storage_dir):
        self.storage_dir = storage_dir
        self.signal_dir = os.path.join(storage_dir, 'signal_slices')
        self.noise_dir = os.path.join(storage_dir, 'noise_slices')
        
        if not os.path.isdir(storage_dir):
            print('Creating empty directory.')
            os.mkdir(storage_dir)
        if not os.path.isdir(self.signal_dir):
            os.mkdir(self.signal_dir)
        if not os.path.isdir(self.noise_dir):
            os.mkdir(self.noise_dir)
        if not os.path.isfile(os.path.join(storage_dir, 'db.sqlite')):
            print('No SQL database built yet, initiating one.')
            #TODO: Actually do that
        
        self.conn = sqlite3.connect(os.path.join(storage_dir, 'db.sqlite'))    
        
        self.SignalSlicer = Slicer(self.signal_dir, type='signal')
        self.NoiseSlicer = Slicer(self.noise_dir, type='noise')
    
    def get_df(self): 
        c = self.conn.cursor()
        list_recs = []
        for file in os.listdir(self.signal_dir):
            rec_id = file.split('_')[0]
            species = sql_selectors.lookup_species_by_rec_id(c, rec_id)
            list_recs.append((file, species))   
        df = pd.DataFrame(list_recs, columns=['path', 'label'])
        return df
        
    def get_class_balances(self):
        df = self.get_df()
        balances = df.groupby('label').path.count().sort_values()
        balances.median()
        return balances
    
    def resample_df(self, df, samples_per_class):
        """ Up- or downsample a dataframe by randomly picking a fixed number of 
        samples for each class """
        out = df.groupby('label').apply(lambda x: x.sample(n=samples_per_class, replace=True)).reset_index(drop=True)
        return out.sample(frac=1).reset_index(drop=True)
        
    def split(self):
        pass

    def inventory(self):
        files = os.listdir(self.signal_dir)
        rec_ids = [file.split('_')[0] for file in files]
        self.counts = {k: v for k,v in zip(Counter(rec_ids).keys(), Counter(rec_ids).values())}
        
    def SQL_lookup(self):
        pass
        
    def prepare_noise(self, audio):
        pass


dbm = DatabaseManager('storage')

df = dbm.get_df()
df
