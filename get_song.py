from bs4 import BeautifulSoup
import requests
import json

def get_bird_songs(genus):
    url = "https://www.xeno-canto.org/api/2/recordings?query="+genus
    page = requests.get(url)
    data = page.json()
    return data
    #soup = BeautifulSoup(page.text, 'html.parser')
    #recordings_dict = json.loads(soup)
    #print(recordings_dict['numRecordings'])
