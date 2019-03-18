import os

def lookup_species_by_rec_id(c, rec_id):
    c.execute("""
        SELECT r.taxonomy_id, t.genus, t.species FROM recordings as r 
        JOIN taxonomy as t ON r.taxonomy_id = t.id
        WHERE r.id = ?
        """, (rec_id,))
    fetch = c.fetchone()
    return fetch[1] + "_" + fetch[2]

# Used to select german bird recordings to download
query = '''SELECT r.id, r.file
    FROM taxonomy AS t
    JOIN recordings AS r ON t.id = r.taxonomy_id
    WHERE t.german = 1.0 AND downloaded IS NULL'''
    
# Used to select a step1 subset of recordings to download
query = '''SELECT r.id, r.file
    FROM taxonomy AS t
    JOIN recordings AS r ON t.id = r.taxonomy_id
    WHERE step1 = 1'''

# we choose to only consider recordings that are small enough to handle i.e.
# smaller than 100 seconds
query = ''' SELECT t.id AS species_id, r.xeno_canto_id, t.genus, t.species, r.id, r.scraped_duration AS duration
    FROM recordings r 
    JOIN taxonomy t ON r.taxonomy_id = t.id
    WHERE t.german = 1.0 AND r.scraped_duration < 150 AND r.scraped_duration > 10
    '''

# Used to set files to 'downloaded' if in rec_id_list
def set_downloaded():
    file_list = os.listdir(STORAGE_DIR)
    id_list = [x[:-4] for x in file_list]
    c.execute('UPDATE recordings SET downloaded = 1.0 WHERE id IN ' +
              str(tuple(id_list)))


# Used to flag files as selected for step1
def update_step1(df_filtered):
    ids = tuple(df_filtered['id'].tolist())
    updatate_step1 = '''UPDATE recordings
                        SET step1 = 1
                        WHERE id IN '''
    c.execute(updatate_step1 + str(ids))
    conn.commit()

# Get df of paths for pickled slices
def get_df(): 
    conn = sqlite3.connect('storage/db.sqlite')
    c = conn.cursor()
    def lookup(id):
        c.execute("""SELECT r.taxonomy_id, t.genus, t.species FROM recordings as r 
            JOIN taxonomy as t 
                ON r.taxonomy_id = t.id
            WHERE r.id = ?""", (id,))
        fetch = c.fetchone()
        return fetch[1] + "_" + fetch[2]
    list_recs = []
    for dirpath, dirname, filenames in os.walk('storage/slices'):
        for name in filenames:
            path = os.path.join(dirpath, name)
            id = dirpath.split("/")[2]
            species = lookup(id)
            list_recs.append((str(path), species))   
    df = pd.DataFrame(list_recs, columns=['path', 'label'])
    return df
