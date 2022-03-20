# Author:   Andy Rabone
# Date:     20/03/2022
# Summary:  - Define archive and extracted files location after file download (default to current working directory)
#           - Housekeeping routine to remove any files from previous runs
#           - wget utility is used to grab the file from URL defined in name_link
#           - Check is made to check a file exists in target location (NB. HTTPError exception object should be 
#             caught and inspected in error handling around call of wget.Download() if raised. In practice, 
#             for transient server side errors exponential back-off would be implemented to facilitate retries.)
#           - Extraction of TSV file from Gzip archive on the filesystem, as per target_archive_location and 
#             target_file_location
#           - TSV file loaded to Pandas dataframe, and subset to living persons only.
#           - Subset filtered to primaryProfessions with 'producer' is found at index 0.
#           - Count taken of filtered subset, assigned to variable and returned in user friendly form to the terminal.

import wget
import pandas as pd
import gzip
import shutil
import os
import os.path

def file_cleanup(file_path):
    """
    Simple function to delete file where it already exists.
    """
    if os.path.exists(file_path):
        os.remove(file_path)


target_archive_location = f"{os.getcwd()}/name.basics.tsv.gz"
target_file_location = f"{os.getcwd()}/name.basics.tsv"
name_link = 'https://datasets.imdbws.com/name.basics.tsv.gz'

for file_path in (target_archive_location, target_file_location):
    file_cleanup(file_path)

wget.download(name_link, out=target_archive_location)

if not os.path.exists(target_archive_location):
    raise Exception("File '{}' failed to download to the target location.".format(name_link))

with gzip.open(target_archive_location, 'rb') as f_in:
    with open(target_file_location, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

df = pd.read_csv(target_file_location, sep='\t')
df = df.query(r"deathYear == '\\N'", engine='python')

df = df[['primaryName','primaryProfession']]
producers = df[df['primaryProfession'].str.find('producer') == 0] 
producers_count = producers['primaryProfession'].count()

print(f"Living persons with 'Producer' as their first primary profession: {producers_count}")