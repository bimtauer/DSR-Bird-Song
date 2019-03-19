import os
from urllib.request import urlcleanup
from birdsong.data_preparation.balanced_split import make_split
from Testing_signal_noise import Slicer
from collections import Counter
import pandas as pd
import sqlite3
import sql_selectors
import datetime

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
        """ Retrieves class name for each slice currently in signal_dir """
        c = self.conn.cursor()
        list_recs = []
        for file in os.listdir(self.signal_dir):
            rec_id = file.split('_')[0]
            species = sql_selectors.lookup_species_by_rec_id(c, rec_id)
            list_recs.append((file, species))   
        df = pd.DataFrame(list_recs, columns=['path', 'label'])
        return df
        
    def get_balances(self):
        """ Retrieves Dataframe with class names for currently available slices
        and groups by class """
        df = self.get_df()
        return df.groupby('label').path.count().sort_values()
    
    def download_below_median(self, max_classes=None, max_recordings=10):
        """ Collects class names for which the number of slices is below median
        number of slices and retrieves rec_ids and urls for the first 10 recordings
        for each class that have not been downloaded yet. """
        c = self.conn.cursor()
        balances = self.get_balances()
        below_median = balances.index.values[balances < balances.median()]
        if max_classes is None:
            max_classes = len(below_median)
            
        print(f'Fetching recordings for {len(below_median)} classes.')
        recordings = []
        running_low = []
        for label in below_median[:max_classes]:
            recordings_for_class = sql_selectors.lookup_recordings_to_download(c, label, max_recordings)
            if len(recordings_for_class) < 10:
                running_low.append(label)
            recordings += recordings_for_class
        print(f'Selected {len(recordings)} recordings for slicing.')
        
        at_a_time = 24
        for bunch in [recordings[i:i+at_a_time] for i in range(0, len(recordings), at_a_time)]:
            self.SignalSlicer(bunch)
            urlcleanup()
        new_balances = self.get_balances()
        differences = new_balances - balances
        self._plot_difference(balances, differences)
        sql_selectors.set_downloaded(recordings)
    
    def _plot_difference(self, balances, differences):
        """ Stores a plot showing the class distribution before and after 
        downloading new slices """
        now = datetime.datetime.now()
        df = pd.DataFrame({'before' : balances, 'added' : differences})
        df.plot(kind='bar', stacked=True)
        import matplotlib.pyplot as plt
        plt.savefig(f'Class balances {now}.pdf', bbox_inches = "tight")
        


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
