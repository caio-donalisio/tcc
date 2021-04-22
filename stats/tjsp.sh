#!/bin/bash

DATE=$(date)
PAGE=$(cat ../crawlers/tjsp/session_page_number)
PDFS=$(find ../crawlers/tjsp/ -iname "*.pdf" | wc -l)
LAST_DOC=$(($PAGE * 20))
FIRST_DOC=$((LAST_DOC - 19))

echo -e "[TJSP]\n"
echo -e "${DATE}\n"
echo -e "Search page: ${PAGE}"
echo -e "Search documents #${FIRST_DOC}-${LAST_DOC}"
echo -e "Number of downloaded PDFs: ${PDFS}"
echo -e "--------------------------------------------"


