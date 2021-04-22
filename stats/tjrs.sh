#!/bin/bash

DATE=$(date)
DOC_DATE=$(cat ../crawlers/tjrs/*date*)
PDFS=$(find ../crawlers/tjrs/ -iname "*.doc" | wc -l)

echo -e "[TJRS]\n"
echo -e "${DATE}\n"
echo -e "Sentence date #${DOC_DATE}"
echo -e "Number of downloaded PDFs: ${PDFS}"
echo -e "--------------------------------------------"


