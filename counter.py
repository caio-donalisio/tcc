COURT = 'tjmg'

import os
for year in range(2016,2023):
    for month in range(1,13):
        os.system(f'gsutil -m du -e "*.html" gs://inspira-production-buckets-{COURT}/{year}/{month:02} | wc -l')