#!/usr/bin/env python3
# -*- coding: latin-1 -*-

import sys
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
import time
import json
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

sys.path.append('../libs/')

from json_to_sqlite import json_to_sqlite_single, sqlite_increment_dupcount_processo

print ("[+] Starting...")

driver = webdriver.Chrome()
driver.implicitly_wait(20)
wait = WebDriverWait(driver, 15)

s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

# set these up... if, in future, we add a 'restart from' feature these will be modified further on
doc_num = 1
page_num = 1

#driver.get('http://www.tjrs.jus.br/site/busca-solr/index.html?aba=jurisprudencia')
#driver.get('https://www.tjrs.jus.br/buscas/jurisprudencia/?aba=jurisprudencia&open=sim&ajax=null')
driver.get('https://www.tjrs.jus.br/buscas/jurisprudencia/?aba=jurisprudencia&open=sim')

time.sleep(1)
#iframe = driver.find_element_by_id('iFrame1')
#driver.switch_to.frame(iframe)

#<input type="text" name="q_palavra_chave" id="q_palavra_chave" value="" class="form-control input-buscar-mobile" placeholder="Palavra-chave">
wait.until(EC.presence_of_element_located,((By.ID,'q_palavra_chave')))
wait.until(EC.presence_of_element_located,((By.ID,'btn_buscar_ajax_topo_open')))
wait.until(EC.presence_of_element_located,((By.ID,'filtroacordao')))


sb = None
try:
	sb = driver.find_element_by_id('q_palavra_chave')
except:
	try:
		sb = driver.find_element_by_name('q_palavra_chave')
	except:
		print('Unable to find the search box')
else:
	sb.send_keys('*')

#<input type="checkbox" value="acordao" id="filtroacordao" name="filtroacordao" class="label-filtros input-checkbox">
filtro_acordao = None
try:
	filtro_acordao = driver.find_element_by_id('filtroacordao')
except:
	print('unable to find acordao filter')
else:
	filtro_acordao.click()

start_date = None

if os.path.isfile("tjrs_date_file"):
	f = open("tjrs_date_file")
	
	start_date = f.read()
	
	f.close()
else:
	start_date = "31/12/2020" # -xxx- rename this to end_date!
	
djd = None
try:
	djd = driver.find_element_by_id('data_julgamento_de')
except:
	print("Unable to find datebox 'julgamento de'")
else:
	djd.send_keys("01/01/2010")
	
dja = None
try:
	dja = driver.find_element_by_id('data_julgamento_ate')
except:
	print("Unable to find datebox 'julgamento ate'")
else:
	print("[+] Extending our search to the date: %s" %(start_date))
	dja.send_keys(start_date)
	
#<button class="button btn btn-default button-buscar-btn button-buscar buscar_ajax" type="button" id="" value="ajax">BUSCAR</button>
search_button = None
try:
	search_button = driver.find_element_by_id('btn_buscar_ajax_topo_open')
except:
	print('unable to find search button')
else:
	search_button.click()


		
print('[+] OK')

time.sleep(3)

time.sleep(1)
#driver.switch_to.default_content()
#iframe = driver.find_element_by_id('iFrame1')
#driver.switch_to.frame(iframe)

"""
<li class="page-item seta-paginacao" alt="Próxima Página" title="Próxima Página" ng-click="proxima_pagina()" ng-show="show_seta_pagina_posterior">
                           <span><i class="fa fa-angle-right"></i>
                           </span>
                        </li>
"""
while True:

	
		
	next_page_button = ''
	
	try:
		main_res = driver.find_element_by_id('main_res_juris')
		print("ok at least we found the main results")
		
		soup = BeautifulSoup(driver.page_source, 'html.parser')
		main_res_divs = soup.find_all('div', {'class': 'result ng-scope'})
		
		for res_divs in main_res_divs:
			dj = {'Tipo Processual': None, 'NumAcordao':None, 'NumProcesso':None, 'Relator(a)': None, 'Orgao Julgador':None, 'Data do Julgamento':None, 'Classe/Assunto':None, 'Requerente':None, 'Requerido':None, 'PathToPdf':None, 'Ementa': None, 'Data da Publicacao':None}
			
			ementa_div = res_divs.find('div', {'class':"col-md-12 col-xs-12"})
			#print(ementa_div)
			ementa_span = ementa_div.find('span', {'class':"firstEmenta text-results conteudoEmentaResultado ng-binding"})
			#print("HI" + ementa_span)
			dj['Ementa'] = ementa_span.get_text()
			#print(dj['Ementa'])
			
			datas_julgamentos = res_divs.find('div', {'class':"col-md-12 result-detail"})#res_divs.find('div', {'class':'row data-julgamento'})
			datas = datas_julgamentos.text.split(':')
			
			data_de_julgamento = datas[1].split()[0]
			data_de_publicacao = datas[2].split()[0]
			
			dj['Data do Julgamento'] = data_de_julgamento
			dj['Data da Publicacao'] = data_de_publicacao
		
			data_de_julgamento_save = data_de_julgamento.replace("-", "/").strip()
			print("DDJS: %s" %(data_de_julgamento_save))
			
			f = open("tjrs_date_file", 'w')
			f.write(data_de_julgamento_save)
			f.flush()
			f.close()
			
			print("DATAS: [%s | %s | %s]" %(datas[0], datas[1], datas[2]))
			
			juris = res_divs.find('div', {'class':'row result-juris'})
			md6s = juris.find_all('div', {'class':'col-md-6'})
			
			duplicate = False
			
			for md6 in md6s:
				if dj['PathToPdf'] == None:
					# Then we have to got to get ourselves a doc
					links_div = md6.find('div', {'class':"col-md-7 hidden_print inteiro-teor-result no_print"})
					doc_link = links_div.find('a')
					teor_link = doc_link['href']
					
					args = teor_link.split('?')[1]
					params = args.split('&')
					processo_num = params[0].split('=')[1]
					dj['NumProcesso'] = processo_num
					
					pdf_file_name = "downloaded_pdfs/%s.doc" %(processo_num)
					dj['PathToPdf'] =  pdf_file_name
					
					if os.path.isfile(pdf_file_name):
						duplicate = True
						break
						
					r = ''
		
					for retries in range(5):
						try:	
							print("Trying to GET %s" %(teor_link))
							r = s.get(teor_link, timeout=(2*(retries+1)))
							#print("Out of GET")
						except:
							# requests.exceptions.Timeout:
							# There are all kinds of exceptions here like OpenSSL.SSL.WantReadError that can trigger before Timeout exception
							# Let's just catch everything and assume that it's some kind of connection / ssl / read error and treat them all the same
							sleep_time = 5 * (retries+1)
							#logging.error("[-] Got a timeout while requesting first_stage_url. Backing off for %d seconds and trying again" %(sleep_time))
							time.sleep(sleep_time)
							continue
			
						if r.status_code == 200:
							break
						else:
							#logging.error("[-] Got a weird status code %d while requesting first_stage_url. Backing off for five seconds and trying again" %(r.status_code))
							time.sleep(5)
							continue
			
					if r == '' or r.status_code != 200:
						#logging.error("Unable to access first_stage_url after 5 tries. Letting watchdog take care of us.")
						driver.quit()
						sys.exit()

					f = open(pdf_file_name, "wb")
					f.write(r.content)
					f.flush()
					f.close()
		
					print("[+] SUCCESS")
					
				#######
				
				juris_rows = md6.find('div', {'class':'row result-juris-rows'})
				md12s = juris_rows.find_all('div', {'class':'col-md-12'})
				
				for md in md12s:
					spans = md.find_all('span')
					#print(spans[0].get_text() + spans[1].get_text())
					
					
					# This is a bad hack and should be fixed at some point
					spans[0] = spans[0].get_text()
					spans[1] = spans[1].get_text()
					
					"""
					Tipo de processo:Habeas Corpus Criminal
					Tribunal:Tribunal de Justiça do RS
					Classe CNJ: Habeas Corpus
					Relator: Glaucia Dipp Dreher
					Redator: 
					Órgão Julgador:Sétima Câmara Criminal
					Comarca de Origem:ALVORADA
					Seção: CRIME
					Assunto CNJ: Roubo Majorado
					Decisão: Acordao
					"""
					if spans[0].startswith('Tipo de processo'):
						dj['Tipo Processual'] = spans[1]
					elif spans[0].startswith('Tribunal'):
						dj['Orgao Julgador'] = spans[1]
					elif spans[0].startswith('Classe CNJ'):
						print("[+] In 'Classe CNJ' --> [%s]" %(spans[1]))
						dj['Classe/Assunto'] = spans[1]
					elif spans[0].startswith('Relator'):
						dj['Relator(a)'] = spans[1]
					elif 'Julgador' in spans[0]:
						dj['Orgao Julgador'] += " / %s" %(spans[1])
					elif spans[0].startswith('Comarca'):
						dj['Orgao Julgador'] += " / %s" %(spans[1])
					elif spans[0].startswith('Assunto CNJ'):
						print("[+] In 'Assunto CNJ' --> [%s]" %(spans[1]))
						dj['Classe/Assunto'] += " / %s" %(spans[1])
					else:
						print("[------] Unexpected entry: %s / %s" %(spans[0], spans[1]))
						#input("> go?")
			
			if duplicate == True:
				sqlite_increment_dupcount_processo('tjrs.db', dj['NumProcesso'])
				continue
							
			dj['Timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
			json_to_write = "%s,\r\n" %(json.dumps(dj))
			#print("%s" %(json_to_write))
		
			jsonf = open('tjrs_json_data.json', 'a')
			jsonf.write(json_to_write)
			jsonf.flush()
			jsonf.close()
			#print(dj)
			
			tdate = dj['Data do Julgamento']
			tdate_parts = tdate.split('-')
			tdate_parts.reverse()
			tdate = '-'.join(tdate_parts)
			dj['Data do Julgamento'] = tdate;
	
			pdate = dj['Data da Publicacao']
			pdate_parts = pdate.split('-')
			pdate_parts.reverse()
			pdate = '-'.join(pdate_parts)
			dj['Data da Publicacao'] = pdate
			
			print("\n[+] Saving to SQL\n")
			#input("go?")
			# Agora joga pro library function json_to_sqlite_single() (single quer dizer que trata com um entry so)
			json_to_sqlite_single('tjrs.db', dj)
			
		print("-----------------------------")
		print("\n\n")
		print("-----------------------------\n\n")
					
					
			
	except Exception as e:
		print("this page is whaq")
		print(e)
	try:
		#next_page_button = driver.find_element_by_xpath("//li[@title='Próxima Página']")
		time.sleep(2)
		next_page_button = driver.find_element_by_xpath("//li[@ng-click='proxima_pagina()']")
		#wait.until(EC.presence_of_element_located,((By.ID,'link_proximo_topo')))
		#next_page_button =  driver.find_element_by_id('link_proximo_topo')
		#print("uh we're trying to click here"
		#next_page_button.click()
	except:
		print("This page or our code is broken no 'next page' button available")
		break
	
		
	print("Next page button found OK!")
	time.sleep(2)
	time.sleep(1)
	#driver.switch_to.default_content()
	#iframe = driver.find_element_by_id('iFrame1')
	#driver.switch_to.frame(iframe)
			
	"""
	#<div id="main_res_juris" class="col-md-10 col-xs-12 div-results-todos" style="display: block;">
	html_data = driver.find_element_by_id('main_res_juris').get_attribute('innerHTML')
	# throw it into beautiful soup then:
        
	find_all('div', {'class':'result ng-scope'}) #<div class="result ng-scope" ng-repeat="resultado in resultados.response.docs" id="linha_result_0">
            <div class="row result-juris">

	"""

		
	"""
	Do we want to log this raw html data here? It would definitely save us from having to recrawl if we wanted to edit something at some points
	"
	
	page_file_path = "raw_html_pages/stj_raw_html_page_%d.html" %(page_num)
	
	if os.path.exists(page_file_path):
		page_file_path = "stj_raw_html_page_%d_retry_at_%s.html" %(page_num, datetime.now().isoformat())
		
	f = open(page_file_path, 'w')
	f.write(html_data)
	f.flush()
	f.close()
	"""


	if 1 == 2:
		aoj.append(dj)
		print(dj)
		doc_num += 1
		
		"""
		Now we're going to overwrite our session file with our current Document number... that way we can recover like Ninjas if we ever need to
		
		sfhandle = open('session_doc_number', 'w')
		sfhandle.write("%d\n" %(doc_num))
		sfhandle.flush()
		sfhandle.close()
		"""

	page_num += 1
	
	#driver.delete_all_cookies() # Wipe cookies just before we click.... fingers cross this'll bypass the recaptcha
	next_page_button.click()
	
	



