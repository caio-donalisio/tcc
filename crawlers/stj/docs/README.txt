To run: python3 stj_crawler.py

To run with a starting index (because the site timed out): python stj_crawler.py [doc_index] 

If you want to clean up existing data before a fresh run then do: rm session_doc_number stj_json_data.json downloaded_pdfs/* raw_html_pages/*

Make sure downloaded_pdfs/ and raw_html_data/ directories exist in the cwd before running.

I'm 99% sure the captcha bypass will work because I tested it manually (i.e. clicking
through to page 19 in my browser, the captcha pops up, I clear cookies, then click 'next page'
and it goes through without having to solve the ReCaptcha). If the crawler doesn't work automagically
and keeps on hitting page 19 (Document 191) over and over no matter how many times it hits 'next page'
then the capture bypass code is broken, probably something with Selenium's clear_all_cookies() method.

