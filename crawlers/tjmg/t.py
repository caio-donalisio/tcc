from bs4 import BeautifulSoup

with open('./crawlers/tjmg/test_html_repetitivos.html','r') as bla:
    html_file = BeautifulSoup(bla)

print(html_file.xpath('./tr'))