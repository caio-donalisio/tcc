#!/bin/bash

DOC=$(cat ../crawlers/tst/tst_document_num)
DATE=$(date)
PDFS=$(find ../crawlers/tst/ -iname "*.pdf" | wc -l)

echo -e "[TST]\n"
echo -e "${DATE}\n"
echo -e "Search documents #${DOC}"
echo -e "Number of downloaded PDFs: ${PDFS}"
echo -e "--------------------------------------------"


