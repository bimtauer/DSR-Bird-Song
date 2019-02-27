#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 14:07:07 2019

@author: tim
"""

"""
This is a PyTorch compatible Dataset class that is initiated from a Dataframe
containing 5 columns: path, label, duration, total signal duration, signal timestamps. 
Those correspond to a soundfile with birdsong, the foreground species, the total 
length of the file, the total length of segments identified as containing bird 
vocalizations, and timestamps of where those vocalizations occur.


    - load an audio file
    - slice and concatenate bird vocalization segments
    - compute a spectrogram of desired type and parameters for these segments
    - slice segments into specified lengths
    - potentially augment slices
    - collate a random selection of slices into a batch

Remaining questions are:
    - How to best handle the one-to-many relationship of audio file to spectrogram slices
    - How to ensure equal class representation while making use of all data
    - How to design loading computationally efficient so that it can be performed
    parallel during training

"""

from torch.utils.data import Dataset
import numpy as np
from .Preprocessing.pre_preprocessing import load_audio, get_signal
from multiprocessing import Process, Queue, Event, active_children
from multiprocessing.pool import ThreadPool
import time

class Preloader(Process):
    def __init__(self, event, queue, task_queue):
        super(Preloader, self).__init__()

        # Bucket list -> list of files that its supposed to get - updated by Dataset
        self.bucket_list = []

        self.e = event
        self.q = queue
        self.t = task_queue

    def run(self):
        while True:
            event_is_set = self.e.wait()
            if event_is_set:
                #print('[Preloader] Refilling bucket...')
                self.bucket_list = self.t.get()
                self.preload_batch()
                self.e.clear()
                #print('[Preloader] Done.')

    def preload_batch(self):
        pool = ThreadPool(min(8, len(self.bucket_list)))
        output = pool.map(self._preload, list(range(len(self.bucket_list))))
        self.q.put(output)

    def _preload(self, i):
        p, l, t, a = self.bucket_list[i]
        audio, sr = load_audio(p)
        signal = get_signal(audio, sr, t)
        try:
            assert len(signal) >= a
        except AssertionError:
            print(f'Issue with file {p}, supposed to be at least {a} long, but only {len(signal)}')
        return (l, list(signal))

class SoundDataset(Dataset):
    def __init__(self, df, **kwargs):
        """ Initialize with a dataframe containing:
        (path, label, duration, total_signal, timestamps)
        kwargs: batchsize = 10, window = 1500, stride = 500, spectrogram_func = None, augmentation_func = None"""

        self.df = df
        #self.df['loaded'] = 0
        self.sr = 22050

        for k,v in kwargs.items():
            setattr(self, k, v)

        # Window and stride in samples:
        self.window = int(self.window/1000*self.sr)
        self.stride = int(self.stride/1000*self.sr)

        # Stack - a list of continuous audio signal for each class
        self.stack = {label:[] for label in set(self.df.label)}
        self.classes = len(set(self.df.label))

        # Instantiate Preloader:
        e = Event()
        self.q = Queue()
        self.t = Queue()
        self.Preloader = Preloader(e, self.q, self.t)
        self.Preloader.start()
        
        # Compute total of available slices
        self.length = self.compute_length()
        
        # Compute output size
        self.shape = self.compute_shape()
        
        # Prepare the first batch: 
        print('Preloading first batch... this might take a moment.')
        self.request_batch(0)


    def __len__(self):
        """ The length of the dataset is the (expected) maximum number of bird vocalization slices that could be
        extracted from the sum total of vocalization parts given a slicing window
        and stride. Calculated by self.compute_length()"""
        return self.length

    def __getitem__(self, i):
        """ Indices loop over available classes to return heterogeneous batches
        of uniformly random sampled audio. Preloading is triggered and the end
        of each batch and supposed to run during training time. At the beginning
        of a new batch, the preloaded audio is received from a queue - if it is 
        not ready yet the main process will wait. """
        # Subsequent indices loop through the classes:
        y = i % self.classes                                                   
        
        # If were at the end of one batch, request next:
        if (i + 1) % self.batchsize == 0:
            self.request_batch(y+1)
        
        # If were at the beginning of one batch, update stack with Preloader's work
        if i % self.batchsize == 0:
            self.receive_request()
        
        # Get actual sample and compute spectrogram:
        audio = self.retrieve_sample(y)
        X = self.spectrogram_func(audio)                                           
        
        try:
            X.shape == self.shape
        except:
            import pdb; pdb.set_trace()
        
        # Normlize:
        X -= X.min()
        X /= X.max()
        X = np.expand_dims(X, 0)
        
        #TODO: Process to check for which files to augment:
        """
        if self.augmentation_func not None:
            X = self.augmentation_func(X)
        """
        return X, y
    
    def retrieve_sample(self, k):
        """ For class k extract audio corresponding to window length from stack
        and delete audio corresponding to stride length"""
        X = self.stack[k][:self.window]
        try: 
            assert len(X) == self.window
        except AssertionError:
            import pdb; pdb.set_trace()                                   
        
        self.stack[k] = np.delete(self.stack[k], np.s_[:self.stride])          
        return X
    
    def request_batch(self, y):
        """ At a given y in classes, look ahead how many times each class k will
        have to be served for the next batch. Request a new file for each where
        only one serving remains. 
        If requests are necessary, send them to Preloader.
        """
        samples_needed = self.compute_need(y)
        bucket_list = []
        for k, s in samples_needed.items():
            required_audio = self.check_stack(k, s)
            if required_audio > 0:
                requests = self.make_request(k, required_audio)
                print(f'Need {required_audio} for class {k}, loading {len(requests)} file(s)')
                for request in requests:
                    bucket_list.append(request)
                
        if len(bucket_list) > 0:
            print('Making request')
            self.t.put(bucket_list)   #update the bucket list
            self.Preloader.e.set()
            self.made_request = True
        else:
            print(f'Still enough samples:')
            self.inventory()
    
    def compute_need(self, y):
        next_batch = range(y, y + self.batchsize)
        samples_needed = {}
        for i in next_batch:
            k = i % self.classes
            if k in samples_needed.keys():
                samples_needed[k] += 1
            else:
                samples_needed[k] = 1
        return samples_needed            

    def check_stack(self, k, s):
        """ Return true if the audio on stack for class k does only suffice for 
        a more serves, false if otherwise """
        required_audio = self.window + ((s-1) * self.stride) + 20000            # Safety buffer #TODO: find out why this helps
        remaining_audio = len(self.stack[k])
        if remaining_audio < required_audio:
            return required_audio - remaining_audio
        else:
            return 0

    def receive_request(self):
        """ Check if Preloader has already filled the Queue and if so, sort data
        into stack."""
        if self.made_request:
            while self.q.empty():
                time.sleep(0.5)
            print('Queue ready - updating stack.')
            new_samples = self.q.get()
            for sample in new_samples:
                label = sample[0]
                self.stack[label] = np.append(self.stack[label], (sample[1]))
            self.made_request = False
        
    def make_request(self, k, a):
        """ For class k sample a random corresponding sound file and return a
        tuple of path, label, timestamps required by the preloader. """
        request = []
        audio_to_preload = 0
        while audio_to_preload < a:
            sample = self.df[self.df.label == k].sample(n=1)
            path = sample.path.values[0]
            label = sample.label.values[0]
            timestamps = sample.timestamps.values[0]
            audio_samples = sample.total_signal.values[0] * self.sr * 0.9 
            request.append((path, label, timestamps, audio_samples))
            audio_to_preload += audio_samples
        return request
    
    def inventory(self):
        for k,v in self.stack.items():
            print(f'Class {k}: {len(v)} audio left')

    def compute_length(self):
        """ Provide an estimate of how many iterations of random, uniformly 
        sampled audio are needed to have seen each file approximately once. """
        sum_total_signal = sum(self.df.total_signal) * 22050
        max_samples = ((sum_total_signal - self.window) // self.stride) + 1
        return int(max_samples)
    
    def compute_shape(self):
        dummy = np.random.randn(self.window)
        spec = self.spectrogram_func(dummy)
        return spec.shape