#!/bin/bash

#if [ "$#" -ne 2 ]; then
#  echo "Usage: $0 [bucket] [file_format]"
#  exit 1
#fi

COURT="$1"
FILE_FORMAT="$2"
START_YEAR="$3"
END_YEAR="$4"



for y in {2018..2020}; do
  for m in {01..12}; do
#	echo "gsutil -m ls gs://inspira-production-buckets-$COURT/$y/$m/*$FILE_FORMAT | wc -l"
	NUM_FILES=$(gsutil -m ls "gs://inspira-production-buckets-$COURT/$y/$m/*$FILE_FORMAT" | wc -l)
	echo $NUM_FILES 
	done
done
