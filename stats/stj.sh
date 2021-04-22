#!/bin/bash

DATE=$(date)
DOC=$(cat ../crawlers/stj/session_doc_number)
PDFS=$(find ../crawlers/stj/ -iname "*.pdf" | wc -l)

echo -e "[STJ]\n"
echo -e "${DATE}\n"
echo -e "Search documents #${DOC}"
echo -e "Number of downloaded PDFs: ${PDFS}"
echo -e "--------------------------------------------"


