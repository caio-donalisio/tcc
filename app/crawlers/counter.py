import os
COURT = 'trf1'
START_YEAR = 2019
END_YEAR = 2022

for year in range(START_YEAR, END_YEAR + 1):
  for month in range(1, 13):
    os.system(f'gsutil -m du gs://inspira-production-buckets-{COURT}/{year}/{month:02} | wc -l')
