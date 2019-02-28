import os
import urllib.request

files_dir = os.path.join('storage', 'german_birds')

def download_recordings(ids):
    for id_rec in ids:
        try:
            urllib.request.urlretrieve("http://www.xeno-canto.org/" 
                + str(id_rec) + "/download",
                os.path.join(files_dir, str(id_rec) + ".mp3"))
            print(f'file {id_rec} downloaded')
        except urllib.error.HTTPError:
            print(f'file {id_rec} not found, trying again')
            try: 
                urllib.request.urlretrieve("http://www.xeno-canto.org/" 
                    + str(id_rec) + "/download",
                    os.path.join(files_dir, str(id_rec) + ".mp3"))
                print(f'file {id_rec} downloaded')
            except:
                print(f'file {id_rec} not found')
            pass