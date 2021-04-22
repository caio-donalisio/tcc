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
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import logging

sys.path.append('../libs/')

from json_to_sqlite import json_to_sqlite_single

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
driver.get('https://jurisprudencia.tst.jus.br/') # Manda o Chrome pro pagina de busca inicial

time.sleep(5)
webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform() # Isso limpa a tela e tira aquele pop-up chato

print('OK GOING NOW')

sb = driver.find_element_by_xpath("//input[@id='standard-with-placeholder']") # localizar o textbox de busca
sb.send_keys('para') # busca para uma palavra que da o maximo de resultados

todos = driver.find_element_by_xpath("//input[@value='todos']") # localizar o check box para 'TODOS'
todos.click() # De-select

acordaos = driver.find_element_by_xpath("//input[@value='acordaos']") # localizar o check box para 'acordaos'
acordaos.click() # select 

#<button class="button btn btn-default button-buscar-btn button-buscar buscar_ajax" type="button" id="" value="ajax">BUSCAR</button>
search_button = driver.find_element_by_xpath("//button[@style='margin: 10px;']") # Localizar o botao de buscar

search_button.click() # e click pra buscar .....

		
print('[+] OK')

time.sleep(5)
	
print('[+] OK going again')

while True:

	
	next_page_button = ''
	
	try:
		next_page_button = driver.find_element_by_xpath("//button[@aria-label='Next Page']") # Tenta localizar o Botao 'Next page', veja o Except: tb

		soup = BeautifulSoup(driver.page_source, 'html.parser')

		#data_divs = soup.find_all('div', {'style':'display: flex; flex-direction: column; padding-bottom: 20px;'})
		table_rows = soup.find_all('tr', {'tabindex':"-1"})
		print("Found %d table rows" %(len(table_rows)))

		for row in table_rows:
			dj = {'Tipo Processual': None, 'NumAcordao':None, 'NumProcesso':None, 'Relator(a)': None, 'Orgao Julgador':None, 'Data do Julgamento':None, 'Classe/Assunto':None, 'Requerente':None, 'Requerido':None, 'PathToPdf':None}

			data_div = row.find('div', {'style':'display: flex; flex-direction: column; padding-bottom: 20px;'})
			link_div = data_div.nextSibling
	
			acordao = data_div.find('a')
			acordao_text = acordao.get_text().strip()
			dj['Tipo Processual'] = acordao_text.split(' - ')[0]
			dj['NumAcordao'] = acordao_text.replace(' ', '')
			pdf_filename = "downloaded_pdfs/%s.pdf" %(dj['NumAcordao'])
			dj['PathToPdf'] = pdf_filename
	

			
			#print(acordao_text)
	
			pdf_span = link_div.find('span', {'title':"Fazer download do inteiro teor no formato PDF"})
			#print(pdf_span)
	
			teor_link = pdf_span.find('a')
			teor_link = teor_link['href']
			#print(teor_link)
			link_args = teor_link.split('?')[1]
			num_proc = link_args.split('&')[1]
			num_proc_int = num_proc.split('=')[1]
			
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
				logging.error("Unable to access first_stage_url after 5 tries. Letting watchdog take care of us.")
				driver.quit()
				sys.exit()

			data = r.text
			f = open(pdf_filename, "wb")
			f.write(r.content)
			f.flush()
			f.close()
			
	
			dj['NumProcesso'] = num_proc_int
	
	
			# Ok, a Ementa aqui e' muito chato, veja o que temos que fazer aqui:
			# ementa_tag = row.find('p', {'class':'Ementa'}) <-- isso falhou onde tinha mais que um <p>..... arghhh
			# entao vamos fazer certo e iterar sobre todos os pargrafos
			
			row_tds = row.find_all('td')
			ementa_td = row_tds[2]
			ementa_paras = ementa_td.find_all('p')
			
			dj['Ementa'] = ''
			for ep in ementa_paras:
				dj['Ementa'] += ' '.join(ep.get_text().split())
	

			# Agora podemos puxar todos os outros detalhes que precisamos pro nosso DB
			tdivs = data_div.find_all('div')

			for tdiv in tdivs:
				t_text = tdiv.get_text().strip()
				parts = t_text.split(':')
				if parts[0] == 'Ementa':
					dj['Ementa'] = "--- ERROR DOUBLE EMENTA HUH? ---" #parts[1]
					print(dj['Ementa'])
					time.sleep(3)
				elif 'Judicante' in parts[0]:
					dj['Orgao Julgador'] = parts[1]
				elif parts[0].startswith('Relator'):
					dj['Relator(a)'] = parts[1]
				elif parts[0] == 'Julgamento':
					dj['Data do Julgamento'] = parts[1]
				elif parts[0].startswith('Publica'):
					dj['Data da Publicacao'] = parts[1]
		
			# So por motivos de controle interno queremos saber quando foi que recordamos esse entry
			dj['Timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
			
			# E agora a gente concatonar ao arquivo do JSON
			json_to_write = "%s,\r\n" %(json.dumps(dj))
			print("%s" %(json_to_write))
		
			jsonf = open('tst_json_data.json', 'a')
			jsonf.write(json_to_write)
			jsonf.flush()
			jsonf.close()
			
			#
			# Ok agora eu vou fazer magica e converter a linha de json que acabei de concatonar ao arquivo JSON em SQL
			# e inserir direito do slite db pra esse TJ
			#
			tdate = dj['Data do Julgamento']
			tdate_parts = tdate.split('/')
			tdate_parts.reverse()
			tdate = '-'.join(tdate_parts)
			dj['Data do Julgamento'] = tdate;
	
			pdate = dj['Data da Publicacao']
			pdate = pdate.split(' ')[1]
			pdate_parts = pdate.split('/')
			pdate_parts.reverse()
			pdate = '-'.join(pdate_parts)
			dj['Data da Publicacao'] = pdate
			
			json_to_sqlite_single('tst.db', dj)
		
		"""
		
		Problema: Usar Selenium para navigar nao deixa a gente comecar de novo onde paramos se der um Crash no crawler
		
		Solucao: Para de mexer direto no site e fica usando mais o backend REST API.
			 
		Ok, veja em baixo os links do REST API..... sempre vai OPTIONS/POST, OPTIONS/POST 
		o POST retorna um JSON que tem todos os detalhes necessario para nosso crawler. Nao precisamos
		parsar o website em si. Porem, a Ementa e o Teor Inteiro embedded no JSON sao em formato HTML
		entao vamos precisar meter o Beautiful Soup neles para tudo funcionar.
		
		Notes:
		1) Acho que o OPTIONS / HTTP/1.1 call nao seja necessario
		2) Nao sei o que e' esse ?a= MAS parece que nao importa o que eu bota ai, tudo continua funcionando .....
		
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/1/1?a=0.13858307151849136
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/1/1?a=0.13858307151849136
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/1/20?a=0.6330306086741528
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/1/20?a=0.6330306086741528
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/21/20?a=0.29064143662225383
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/21/20?a=0.29064143662225383
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/41/20?a=0.724379876798721
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/41/20?a=0.724379876798721
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/61/20?a=0.5098526139438505
		https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/61/20?a=0.5098526139438505
		"""
		
		# Porem, ate eu faco versao 2.0 desse crawler ainda estamos usando o Selenium para a navigacao
		next_page_button.click()
		time.sleep(2)
	except Exception as e:
		# Podemos terminar aqui para um monte de motivo mas o mais comum e' porque nao achamos o botao de 'Next Page'
		# ou seja, esse botao nao tinha carregado ainda .... 
		print("FAILED\n")
		print(e)
		break