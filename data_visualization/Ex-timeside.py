"""
Generate a Spectrogram image for a given WAV audio sample.

Compatible with several audio formats: wav, flac, mp3, etc.
Requires: https://code.google.com/p/timeside/

@Satyan
"""

import wave

import pylab


def graph_spectrogram(wav_file):
    sound_info, frame_rate = get_wav_info(wav_file)
    pylab.figure(num=None, figsize=(19, 12))
    pylab.subplot(111)
    pylab.title('spectrogram of %r' % wav_file)
    pylab.specgram(sound_info, Fs=frame_rate)
    pylab.savefig('spectrogram.png')


def get_wav_info(wav_file):
    wav = wave.open(wav_file, 'r')
    frames = wav.readframes(-1)
    sound_info = pylab.fromstring(frames, 'Int16')
    frame_rate = wav.getframerate()
    wav.close()
    return sound_info, frame_rate


if __name__ == '__main__':
    wav_file = 'Falcon-Mark_Mattingly-169493032.wav'
    graph_spectrogram(wav_file)
