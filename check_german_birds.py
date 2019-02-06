import pandas as pd
import json

birds = pd.read_csv('german_birds.csv')

with open('taxonomy.txt') as f:
    d_string = f.read()

d = json.loads(d_string)

def in_db(row):
    try:
        d[ row['order'] ][ row['family'] ][ row['genus'] ][ row['genus']+' '+row['species'] ]
        return True
    except:
        return False

birds['found'] = birds.apply( lambda row: in_db(row), axis=1)

birds_in_db = birds.loc[birds['found'],:]
birds_in_db = birds_in_db.loc[:, ['order', 'family', 'genus', 'species']  ].reset_index(drop=True)

birds_in_db.to_csv('german_birds_in_db.csv')