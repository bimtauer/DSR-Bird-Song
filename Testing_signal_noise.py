#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 16:17:13 2019

@author: tim
"""

from urllib.request import urlretrieve, urlcleanup
import librosa
import numpy as np
from birdsong.data_preparation.audio_conversion.signal_extraction import normalized_stft, median_mask, morphological_filter, indicator_vector

url = 'https://www.xeno-canto.org/461383/download'


def load_tmp(url):
    temp_file = urlretrieve(url)[0]
    audio, sr = librosa.load(temp_file, sr = 22050, mono=True)
    return audio

audio = load_tmp(url)

# Signal
stft = normalized_stft(audio)
mask = median_mask(stft, 3)
morph = morphological_filter(mask)
vec = indicator_vector(morph)
ratio = audio.shape[0] // vec.shape[1] #Results in miniscule time dilation of ~0.001 seconds but is safe
vec_stretched = np.repeat(vec, ratio).astype(bool)

signal_indeces = np.where(vec_stretched)[0]
noise_indeces = np.where(~vec_stretched)[0]

signal = audio[signal_indeces]
noise = audio[noise_indeces]



audio = audio[:299446]