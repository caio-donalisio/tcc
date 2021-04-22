#!/usr/bin/env python3
# -*- coding: latin-1 -*-

import sys
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
import time
import json
from bs4 import BeautifulSoup
import requests
import find_aa_stj
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging

sys.path.append('../libs/')

from parse_multi_pets import handle_multiple_petitions

from json_to_sqlite import json_to_sqlite_single, sqlite_increment_dupcount

def handle_unhandled_exceptions(exctype, value, traceback):
	global driver
	global start_time
	global start_again
	global doc_num
	
	end_time = time.time()
	logging.error("\n\n\n\n------------------------\n\n[--- UNHANDLED EXCEPTION ALERT ---]\n\nHi! This crawler has behaved in a very naughty way and as such has failed to handle an exception in the correct manner.\nI am now handling this by closing down the crawler's driver object (hopefully) closing the browser and exiting in a graceful manner.\nIf I was started by the Watchdog then he should restart me within the next few seconds.\nThank you for (ab)using python's sys.excepthook!\n\n")
	logging.error("We ran for %d seconds and handled %d search results" %(end_time-start_time, doc_num - start_again))
	logging.error("------------------------\n\n")
	logging.error("\n\n")
	logging.error("Uncaught exception", exc_info=(exctype, value, traceback))
	driver.quit()

print ("[+] Starting...")
sys.excepthook = handle_unhandled_exceptions
cwd = os.getcwd()
logfile = "%s/../../logs/stj/stj.log" %(cwd)
logging.basicConfig(filename=logfile,level=logging.INFO)

logging.info("[+] Starting STJ crawler...")

start_time = time.time()
driver = webdriver.Chrome()
driver.implicitly_wait(20)
wait = WebDriverWait(driver, 15)

s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

# set these up... if, in future, we add a 'restart from' feature these will be modified further on
doc_num = 1
page_num = 1

# Don't ask what an 'ITA' pdf is... I will include examples under docs/ or something
pdf_ita = False

# If these directories don't exist then we're going to need to create them

if not os.path.exists('raw_html_pages'):
	os.mkdir('raw_html_pages')

if not os.path.exists('downloaded_pdfs'):
	os.mkdir('downloaded_pdfs')
	
##############################################

driver.get('https://scon.stj.jus.br/SCON/')

webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()

sb = driver.find_element_by_id("pesquisaLivre")
#sb.send_keys('que OU (1=1)')
sb.send_keys('a OU o OU de')

"""
search_div = driver.find_element_by_id('botoesPesquisa')

search_inputs = search_div.find_elements_by_tag_name('input')

for search_input in search_inputs:
	if search_input.get_attribute('type') == 'submit':
		search_input.click()
		break
"""

sb.send_keys(Keys.RETURN)
		
print('[+] OK')

time.sleep(3)
"""
acordaos = driver.find_element_by_id('campoACOR')

link = acordaos.find_element_by_tag_name('a')

link.click()
"""
time.sleep(2)
start_again = 1	
skip_start = 0


if os.path.isfile('session_doc_number'):
	f = open('session_doc_number', 'r')
	d = f.read()
	skip_start = int(d)
	
while True:

	"""
	If we want to 'restore session', if our crawler crashes at 10000 documents downloaded or smth, then we're going to have to do:
	
	A) We can do it via Python Requests, but at that point we should move STJ crawler away from selenium all together IMO
	
	B) We can figure out how to make Selenium do a POST request... apparently you can something like this:
	
	JavascriptExecutor js = (JavascriptExecutor)driver;
	driver.execute_script("navegaForm('10001');")
	
	^ 'navegaForm is a Javascript function present in the STJ JS codebase and is what is called when you click the NextPage element...
	   so we can just invoke that Javascript function via Selenium and it'll load the page we were at when we crashed
	   in the browser.... from there we can just carry on from the NextPage button.....
	
	In the other case the the POST request we want is like this:
	
	URI:	POST /SCON/jurisprudencia/toc.jsp HTTP/1.1
	
	Params:	tipo_visualizacao=&t=JURIDICO&b=ACOR&p=true&l=10&livre=QUE+OU+%281%3D1%29&i=71&b=ACOR
	"""
	
	
	if skip_start:
		logging.info("We've been given an input param, going to try to use it as a start index for our search...")
		time.sleep(1)
		
		start_again = skip_start
		#JavascriptExecutor js = (JavascriptExecutor)driver;
		driver.execute_script("navegaForm('%d');" %(start_again))
		
		time.sleep(3)
		
		html_data = driver.find_element_by_class_name('listadocumentos').get_attribute('innerHTML')
		
		page_num = start_again / 10
		doc_num = start_again
		skip_start = 0
		
	next_page_button = ''
	
	try:
		next_page_button = driver.find_element_by_class_name('iconeProximaPagina')
	except:
		print("This page or our code is broken no 'next page' button available")
		break
	
		
	logging.info("Next page button found OK!")
	
	"""
	For whatever reason this isnt working.... it never finds the recaptcha element
	
	Going to move the cookie wipe lower down the page
	
	try:
		captcha = driver.find_element_by_id('recaptcha-anchor-label')
	except:
		print("No recaptcha found on this page!")
	else:
		print("Recaptcha found... going to Ninja past it by clearing cookies!")
		driver.delete_all_cookies()
		sleep(2)
	"""
			
	#html_data = driver.find_element_by_id('listadocumentos').get_attribute('innerHTML')
	html_data = driver.find_element_by_class_name('listadocumentos').get_attribute('innerHTML')

	#print("got html_data")
	"""
	Do we want to log this raw html data here? It would definitely save us from having to recrawl if we wanted to edit something at some points
	"""
	
	page_file_path = "raw_html_pages/stj_raw_html_page_%d.html" %(page_num)
	
	if os.path.exists(page_file_path):
		page_file_path = "raw_html_pages/stj_raw_html_page_%d_retry_at_%s.html" %(page_num, datetime.now().isoformat())
		
	f = open(page_file_path, 'w')
	f.write(html_data)
	f.flush()
	f.close()
	
	"""
	So at this point we've grabbed the HTML from the Selenium driver and we pass it off to BeautifulSoup for much more powerful parsing / handling
	"""
	
	soup = BeautifulSoup(html_data, 'html.parser')
	#print("got soup")
	divs = soup.find_all('div', {'class':'documento'})#recursive=False)

	for div in divs:
		dj = {'Tipo Processual': None, 'NumAcordao':None, 'NumProcesso':None, 'Relator(a)': None, 'Orgao Julgador':None, 'Data da Publicacao':None, 'Data do Julgamento':None, 'Classe/Assunto':None, 'Requerente':None, 'Requerido':None, 'PathToPdf':None}
		#print("1")
		#dj = {'NumProcesso':None, 'Relator(a)': None, 'Orgao Julgador':None, 'Data do Julgamento':None, 'Classe/Assunto':None, 'Requerente':None, 'Requerido':None, 'PathToPdf':None}

		linkdiv = div.find('div', {'class':'iconesAcoes d-print-none'})
		linka = linkdiv.find('a')
			
		try:
			linkstr = str(linka).split("javascript:inteiro_teor('")[1]
		except:
			logging.error("Looks like we have a bad result. Skipping this one.")
			continue
			
		#print(linkstr)
		linkstr = linkstr.split("')")[0]
		#print("3")
		params = linkstr.split('&amp;') # I hate html specialchar conversions -- !BEWARE! this may not be needed on other browsers! (e.g. you'd have to use '&')
		num_registro = params[0].split('=')[1]
		dj['NumAcordao'] = num_registro
	
		first_stage_url = "https://scon.stj.jus.br/SCON/GetInteiroTeorDoAcordao?num_registro=%s&dt_publicacao=%s" %(params[0].split('=')[1], params[1].split('=')[1])

		#print(first_stage_url)
	
	
		logging.info("Saving PDF into %s.pdf" %(num_registro))
		pdf_file_name = "downloaded_pdfs/%s.pdf" %(num_registro)
		dj['PathToPdf'] = pdf_file_name
		
		if os.path.isfile(pdf_file_name):
			sqlite_increment_dupcount('stj.db', dj['NumAcordao'])
		else:
			r = ''
		
			for retries in range(5):
				try:	
					#print("Trying to GET %s" %(first_stage_url))
					r = s.get(first_stage_url, timeout=(2*(retries+1)))
					#print("Out of GET")
				except:
					# requests.exceptions.Timeout:
					# There are all kinds of exceptions here like OpenSSL.SSL.WantReadError that can trigger before Timeout exception
					# Let's just catch everything and assume that it's some kind of connection / ssl / read error and treat them all the same
					sleep_time = 5 * (retries+1)
					logging.error("[-] Got a timeout while requesting first_stage_url. Backing off for %d seconds and trying again" %(sleep_time))
					time.sleep(sleep_time)
					continue
			
				if r.status_code == 200 and not 'Erro ao gerar arquivo do inteiro teor' in r.text: # Will this work?
					break
				else:
					if 'Erro ao gerar arquivo do inteiro teor' in r.text:
						logging.error("[-] Ok we found an Erro ao gerar arquivo do inteiro teor error! \o/ Go Team \o/")
					else:
						logging.error("[-] Got a weird status code %d while requesting first_stage_url. Backing off for five seconds and trying again" %(r.status_code))
					time.sleep(5)
					continue
			
			if r == '' or r.status_code != 200 or 'Erro ao gerar arquivo do inteiro teor' in r.text:
				logging.error("Unable to access first_stage_url after 5 tries. Letting watchdog take care of us.")
				driver.quit()
				sys.exit()

			#print(r.headers['Content-Type'])
		
			if r.headers['Content-Type'] == 'application/pdf':
				f = open(pdf_file_name, "wb")
				f.write(r.content)
				f.flush()
				f.close()
				#print("[+] SUCCESS")
			elif 'text/html' in r.headers['Content-Type']:
				dj['PathToPdf'] = handle_multiple_petitions(r.content, r.text)
			else:
				logging.error("[-] Unknown content type header in HTTP reponse: %s" %( r.headers['Content-Type']))
			
			data = r.text
		
			"""
			Need to add detection for: "Erro ao gerar arquivo do inteiro teor: Server returned HTTP response code: 500 for URL"
			"""
			if 'Erro ao gerar arquivo do inteiro teor' in data:
				logging.error('[-] Erro ao gerar arquivo do inteiro teor - we need to basically fix this to run over again')

			if pdf_ita:
				agravado, agravante = find_aa_stj.get_aa_from_stj_ita_pdf(pdf_file_name)
				pdf_ita = False
			else:
				agravado, agravante = find_aa_stj.get_aa_from_stj_pdf(pdf_file_name)
		
			dj['Requerido'] = agravado
			dj['Requerente'] = agravante
	
			logging.info("AA - [%s,%s]" %(agravado, agravante))
	
			paragraphs = div.find_all('div', {"class": "paragrafoBRS"})
	
			for paragraph in paragraphs:
				#f.write(paragraph.decode_contents())
				#print(paragraph)
				#title = paragraph.find('h4').find_all(text=True)
				titlep = paragraph.find('div', {"class": "docTitulo"})
				textp = paragraph.find('div', {"class": "docTexto"})
		
				if textp == None:
					textp = paragraph.find('pre', {"class": "docTexto"})
			
				title = titlep.find_all(text=True)
				text = textp.find_all(text=True)
		
				title = ''.join(title)	
				text = ''.join(text)
		
				try:
					if title == 'Processo':
						#print('Processo: %s' %(text))
						processo = text.split('\n')[1].strip()
						tipo_process = processo.split()[0]
						dj['NumProcesso'] = processo#.split("/")[0]
						dj['Tipo Processual'] = tipo_process
					elif title == 'Relator(a)':
						dj['Relator(a)'] = text
					elif title.count("Julgador") == 1:
						dj['Orgao Julgador'] = "Superior Tribunal de Justi\u00e7a - " + text
					elif title == 'Data do Julgamento':
						dj['Data do Julgamento'] = text
					elif title == 'Ementa':
						dj['Ementa'] = text
					elif title.startswith('Data da Publica'):
						dj['Data da Publicacao'] = text
				except:
					logging.error('Major problem processing the following text pair: Title: %s --- Text: %s' %(title, text))
		
			### 2020-08-18 - New policy, we write out the JSON results line by line and we give it a timestamp too.
			dj['Timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
			json_to_write = "%s,\r\n" %(json.dumps(dj))
			#print("%s" %(json_to_write))
		
			jsonf = open('stj_json_data.json', 'a')
			jsonf.write(json_to_write)
			jsonf.flush()
			jsonf.close()
		
			json_to_sqlite_single('stj.db', dj)
		
		doc_num += 1
		
		"""
		Now we're going to overwrite our session file with our current Document number... that way we can recover like Ninjas if we ever need to
		"""
		sfhandle = open('session_doc_number', 'w')
		sfhandle.write("%d\n" %(doc_num))
		sfhandle.flush()
		sfhandle.close()

	page_num += 1

	"""
	Let's write out our JSON data by appending it to the JSON data file.
	
	2020-08-18: See above, we're now writing this out upstairs....
	
	jsonf = open('stj_json_data.json', 'a')
	jsonf.write(json.dumps(aoj))
	jsonf.flush()
	jsonf.close()
	"""
	
	driver.delete_all_cookies() # Wipe cookies just before we click.... fingers cross this'll bypass the recaptcha
	next_page_button.click()
	
print ("Apparently we've run through the whole of STJ.... \o/ /o/ \\o\\")
print("\n\n\n\n")
