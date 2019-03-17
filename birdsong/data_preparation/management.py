import os
from birdsong.data_preparation.balanced_split import make_split
from collections import Counter

class DatabaseManager(object):
    """ This class serves as an abstraction that bundles the various functions
    for acquiring, inventorizing, manipulating and serving data """
    def __init__(self, storage_dir):
        self.storage_dir = storage_dir
        pass


    def resample_df(self, df, samples_per_class):
        """ Up- or downsample a dataframe by randomly picking a fixed number of 
        samples for each class """
        out = df.groupby('label').apply(lambda x: x.sample(n=samples_per_class, replace=True)).reset_index(drop=True)
        return out.sample(frac=1).reset_index(drop=True)
        
    def split(self):
        pass

    def inventory(self):
        files = os.listdir(self.storage_dir)
        rec_ids = [file.split('_')[0] for file in files]
        self.counts = {k: v for k,v in zip(Counter(rec_ids).keys(), Counter(rec_ids).values())}
        
    def SQL_lookup(self):
        pass
        
    def prepare_noise(self, audio)
    
