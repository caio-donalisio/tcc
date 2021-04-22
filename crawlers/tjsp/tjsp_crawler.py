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
import tjsp_parse_pdf
import logging

sys.path.append('../libs/')

from json_to_sqlite import json_to_sqlite_single

#
# Exception handler pra todos os excecoes que nao estao
# dentro de um try/except.
#
def handle_unhandled_exceptions(exctype, value, traceback):
	global driver
	global start_time
	global start_again
	global page_num
	
	end_time = time.time()
	logging.error("\n\n\n\n------------------------\n\n[--- UNHANDLED EXCEPTION ALERT ---]\n\nHi! This crawler has behaved in a very naughty way and as such has failed to handle an exception in the correct manner.\nI am now handling this by closing down the crawler's driver object (hopefully) closing the browser and exiting in a graceful manner.\nIf I was started by the Watchdog then he should restart me within the next few seconds.\nThank you for (ab)using python's sys.excepthook!\n\n")
	logging.error("We ran for %d seconds and handled %d search results" %(end_time-start_time, ((page_num - start_again) * 20)))
	logging.error("------------------------\n\n")
	logging.error("\n\n")
	logging.error("Uncaught exception", exc_info=(exctype, value, traceback))
	driver.quit()


#
# Esse hack aqui e' como esse handler em cima esta installado	
#
sys.excepthook = handle_unhandled_exceptions



cwd = os.getcwd()
logfile = "%s/../../logs/tjsp/tjsp.log" %(cwd)
logging.basicConfig(filename=logfile,level=logging.INFO)

logging.info("[+] Starting TJSP crawler...")

start_time = time.time()

option = webdriver.ChromeOptions()
option.add_argument('headless')
driver = webdriver.Chrome("chromedriver", options=option)
driver.implicitly_wait(20)
wait = WebDriverWait(driver, 15)

s = requests.Session()
#retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
#s.mount('https://', HTTPAdapter(max_retries=retries))

# set these up... 
page_num = 1
start_again = 1
skip_start = 0

#
# Como estamos bem no comeco, usamos agora muito o Selenium.
#
# Premeiro fazer um login para evitar ter que fazer ReCaptcha
#
driver.get(('https://esaj.tjsp.jus.br/sajcas/login?service=https%3A%2F%2Fesaj.tjsp.jus.br%2Fesaj%2Fj_spring_cas_security_check'))
wait.until(EC.presence_of_element_located,((By.ID,'usernameForm')))
user_name = driver.find_element_by_id('usernameForm')
user_name.send_keys('41847268870')
wait.until(EC.presence_of_element_located,((By.ID,'passwordForm')))
user_name = driver.find_element_by_id('passwordForm')
user_name.send_keys('27011992')
driver.find_element_by_id('pbEntrar').click()

#
# Depois carregamos a pagina de busca
#
driver.get('https://esaj.tjsp.jus.br/cjsg/consultaCompleta.do')

#<input type="text" name="dados.buscaInteiroTeor" size="100" value="" formattype="TEXT" formato="120" obrigatorio="" rotulo="Pesquisa livre" onkeypress="CT_KPS(this, event);" onblur="CT_BLR(this);" onkeydown="CT_KDN(this, event);" onmousemove="CT_MMOV(this, event);" onmouseout="CT_MOUT(this, event);" onmouseover="CT_MOV(this, event);" onfocus="C_OFC(this, event);" style="" class="spwCampoTexto " id="iddados.buscaInteiroTeor" title="" alt="">

#
# Esperamos ate varios <input> boxes estao presente e fazemos a nossa
# busca usando o Selenium para selecionar elementos, mandar key press, e mandar
# click em botoes
#
wait.until(EC.presence_of_element_located,((By.ID,'iddados.buscaInteiroTeor')))
search_box = driver.find_element_by_id('iddados.buscaInteiroTeor')
search_box.send_keys('a ou de ou o')

wait.until(EC.presence_of_element_located,((By.ID,'pbSubmit')))
search_button = driver.find_element_by_id('pbSubmit')
search_button.click()
		
logging.info('[+] OK - TJSP Crawler initialized')

were_restarted = False
first_run = True
time.sleep(3)


#
# Aqui vamos ver se tem um sessao para continuar
#	
if os.path.isfile('session_page_number'):
	f = open('session_page_number', 'r')
	d = f.read()
	skip_start = int(d)
	f.close()
	
while True:
	
	start_off_url = ''
	
	#
	# Se estamos re-comecando um sessao tentamos re-comecar onde paramos da ultima vez
	#
	if skip_start:
		logging.info("We've been given an input param, going to try to use it as a start index for our search...")
		time.sleep(1)
		start_again = skip_start
		page_num = skip_start
		
		start_off_url = 'https://esaj.tjsp.jus.br/cjsg/trocaDePagina.do?tipoDeDecisao=A&pagina=%d&conversationId=' %(skip_start)
		logging.error(start_off_url)
		driver.get(start_off_url)
		skip_start = 0
		html_data = driver.page_source
		were_restarted = True
		first_run = False	

	
	#
	# Esse codigo aqui e' um hack dos horrores.....
	# O Javascript frontend do TJSP, enquanto espera um HTTP request a terminar, fica
	# mostrando uma roda rodando tipo aquelle animacao trouxo de Windows 95.
	#
	# Esse 'While True:' aqui honestamente so fica esperando ate o <img> tag do loading image
	# disaparecer..... serio. :/
	#
	if first_run:
		while True:
			divdados = driver.find_element_by_id('divDadosResultado-A')
			try:
				divimages = divdados.find_elements_by_tag_name('img')
			except:
				logging.error('Something has gone very wrong')
				time.sleep(60 * 20)
				break
			else:
				if len(divimages) == 1:
					logging.info('The loading image is still there.... we will sleep and continue')
					time.sleep(1)
				else:
					logging.info('The loading image is gone.... we can continue')
					time.sleep(1)
					break
			
	
		logging.info("found the results, we can parse and go to next page")
	
		wait.until(EC.presence_of_element_located,((By.ID,'tabs')))
	
			
		html_data = driver.find_element_by_id('tabs').get_attribute('innerHTML')

	#
	# Ok, se chegamos aqui temos o HTML e o DOM e tudo e podemos jogar a coisa toda no
	# BeautifulSoup
	#
	logging.info("got html_data")
	
	
	#
	# Premeiro a gente salva o HTML 'cru'. Pq? Pq uma vez que temos baixado tudo, a
	# gente nao quer ter que crawl/baixar tudo de novo caso que percebemos um erro
	# em como a gente processou certo informacao o decidimos que queremos extrair
	# outra informacao. Seja: a gente ja tem tudo o HTML aqui no servidor local
	# e nao tem que fazer o trabalho de crawl/baixar tudo de novo.
	#
	page_file_path = "raw_html_pages/tjsp_raw_html_page_%d.html" %(page_num)
	
	if os.path.exists(page_file_path):
		page_file_path = "raw_html_pages/tjsp_raw_html_page_%d_retry_at_%s.html" %(page_num, datetime.now().isoformat())
		
	f = open(page_file_path, 'w')
	f.write(html_data)
	f.flush()
	f.close()
	
	#
	# Aqui ja carregamos o HTML da pagina dos resultados, ja salvamos num arquivo com nome unico
	# e agora ta a hora de jogar tudo no BeautifulSoup e fazer o trabalho real...
	#
	soup = BeautifulSoup(html_data, 'html.parser')
	
	if first_run:
		div = soup.find('div', {'id':'divDadosResultado-A'})
		table = div.find('table')
		first_run = False
	else:
		table = soup.find('table')
		were_restarted = False

	if table == None:
		logging.error("[!!!] A theoretically impossible error has occured.")
		if first_run:
			logging.error("[!!!] This error has occured during first_run")
			logging.error("[!!!] Does this mean that we are getting 403 errors from initial execution?")
			logging.error("[!!!] Please check the html data in %s for more information that will be useful for debugging" %(page_file_path))
		else:
			logging.error("[!!!] This error has occured during continuous execution")
			logging.error("[!!!] This means that we are now failing to find a <table> tag in HTML data that we already found a <table> tag in! (Some kind of BeautifulSoup bug?")
			logging.error("[!!!] Please check the html data in %s for more information that will be useful for debugging" %(page_file_path))
			
		logging.error("[!!!] In any case I will now exit so that further debugging can occur. Thx.")
		#driver.quit()
		sys.exit()
			
	
	#
	# Infelizmente muito desse codigo so vai fazer sentido olhando o HTML do TJSP.
	# Mas, a base da ideia e' que estamos 'parsing' o HTML usando BeautifulSoup para
	# extrair, para cada acao na pagina que foi carregado:
	#
	# 1) informacoes sobre o acao (Tipo Processual, Numero de Acordao,
	#    Numero de Processo, Relator(a), Orgao Julgdaor, datas de
	#    Julgamento e Publicacao, Classe, Assunto etc.
	#
	# 2) um link que vai deixar o crawler baixar o 'Inteiro Teor' em
	#    formato PDF (por preferencia) ou .doc (MS Word) se so tiver esse formato.
	# 
	# Os nomes dos tags / elements / atributos vao mudar com cada TJ (caso que nao usa
	# um sistema de software padrao tipo PJe ou eSaj). A maioria da batalha e' dominar
	# essa parte.
	#
	tbody = table.find('tbody')

	fcs = tbody.find_all('tr', {'class':'fundocinza1'})
	#print(fcs)
	#print("%d" %(len(fcs)))

	aoj = []
	for fc in fcs:
		#print("In FC")
		dj = {'Tipo Processual': None, 'NumAcordao':None, 'NumProcesso':None, 'Relator(a)': None, 'Orgao Julgador':None, 'Data do Julgamento':None, 'Classe/Assunto':None, 'Requerente':None, 'Requerido':None, 'PathToPdf':None}
		
		ec = fc.find('tr', {'class':'ementaClass'})
		acordao = ec.find('a', {'title':'Visualizar Inteiro Teor'})
	
		#print("NumProcesso: %s" %(acordao.get_text().strip()))
		#print("cdAcordao: %s" %(acordao.get('cdacordao')))
		#print("cdForo: %s" %(acordao.get('cdforo')))
	
		dj['NumAcordao'] = acordao.get('cdacordao')
		dj['NumProcesso'] = acordao.get_text().strip()

			
		pdf_url = "https://esaj.tjsp.jus.br/cjsg/getArquivo.do?cdAcordao=%d&cdForo=%d" %(int(acordao.get('cdacordao').strip()), int(acordao.get('cdforo').strip()))
		
		
		#
		# TODO; Move esse codigo para um download_file.py library porque
		#       todos os crawlers vao fazer algo semelhante e seria melhor
		#       ter o codigo centralizado num lugar em vez de duplicar a
		#       mesma logica em cada crawler.
		#
		logging.info("Saving PDF from %s into %s.pdf" %(pdf_url, dj['NumAcordao']))
		pdf_file_name = "downloaded_pdfs/%s.pdf" %(dj['NumAcordao'])
		dj['PathToPdf'] = pdf_file_name
	
		request_cookies_browser = driver.get_cookies()
		
		for c in request_cookies_browser:
			s.cookies.set(c['name'], c['value'])
		
		#
		# TODO: Principalmente seria esse loop aqui que seria movido pro library... isso e' a base
		#       da logica de baixar o pdf/doc/etc
		#
		for retries in range(5):
			try:	
				#print("Trying to GET %s" %(pdf_url))
				r = s.get(pdf_url, timeout=1)
				#print("Out of GET")
			except:
				# requests.exceptions.Timeout:
				# There are all kinds of exceptions here like OpenSSL.SSL.WantReadError that can trigger before Timeout exception
				# Let's just catch everything and assume that it's some kind of connection / ssl / read error and treat them all the same
				logging.error("[-] Got a timeout trying to download a PDF. Backing off for five seconds and trying again")
				time.sleep(5)
				continue
			
			if r.status_code == 200:
				break
			else:
				logging.error("[-] Got a weird status code %d while downloading a PDF. Backing off for five seconds and trying again" %(r.status_code))
				time.sleep(5)
				continue
			
		#
		# TODO:
		# 
		# O library function retornaria o objeto 'r' e o codigo que chama teria que fazer tipo:
		# if not r or r.status_code != 200:
		#     logging.error("Unable to download PDF. Letting watchdog take care of us.")
		#     sys.exit
		#
		if r.status_code != 200:
			logging.error("Unable to download PDF. Letting watchdog take care of us.")
			driver.quit()
			sys.exit()
			
		# Uma vez que sabemos que baixamos o PDF com exito, podemos salvar	
		f = open(pdf_file_name, "wb") # isso e' Python3 - lembra de abrir PDF (ou doc) em modo 'b' pra binary file
		f.write(r.content) # Assim lembra tambem de user r.content (bytes) e nao r.text (ascii)
		f.flush()
		f.close()
	
		#print("Entering parse_pdf")
		requerentes,requeridos = tjsp_parse_pdf.tjsp_parse_pdf(pdf_file_name) # Isso aqui e' completamente quebrado, mas functiona 10% do tempo
		#print("Exiting parse_pdf")
		dj['Requerido'] = requeridos
		dj['Requerente'] = requerentes
		
		ec2s = fc.find_all('tr', {'class':'ementaClass2'})
	
		# Para cado resultado nesta pagina....
		for ec2 in ec2s:
			td = ec2.find('td')
			text = td.find_all(text=True)
			text = ''.join(text)
			parts = text.split(':')
			title = parts[0].strip()
			texto = parts[1].strip()
		
			try:
				if title == 'Relator(a)':
					dj['Relator(a)'] = texto
				elif title == 'Classe/Assunto':
					class_parts = texto.split('/')
					dj['Tipo Processual'] = class_parts[0]
					dj['Classe/Assunto'] = '/'.join(class_parts[1:])
				elif title.count('julgador'):
					dj['Orgao Julgador'] = "TJSP - " + texto
				elif title == 'Data do julgamento':
					dj['Data do Julgamento'] = texto
				elif title.startswith('Data de publica'):
					dj['Data da Publicacao'] = texto
				elif 'Ementa:' in text:
					#print("FOUND EMENTA")
					#time.sleep(3)
					
					ementa_divs = td.find_all('div')
					if len(ementa_divs) == 1:
						# Essa ementa nao precisava ser expandido, vamos marcar [COMPLETE] por equanto so por debug
						dj['Ementa'] = "[COMPLETE] " + ''.join(parts[1:]).strip()
						print(dj['Ementa'])
						#time.sleep(5)
					else:
						#print("FOUND EMENTA ELSE %d", len(ementa_divs))
						#print(ementa_divs[1])
						working_ementa = ''.join( ementa_divs[1].find_all(text=True) ).strip()
						#print("[ WE: " + working_ementa + ' ]')
						working_ementa_parts = working_ementa.split('Ementa:')[1:]
						#print("WEP" + working_ementa_parts)
						
						fixed_ementa = ''.join(working_ementa_parts).strip()
						# Essa ementa precisava ser expandido, vamos marcar [HIDDEN] por enquanto so por debug
						dj['Ementa'] = "[HIDDEN] " + fixed_ementa
						print(dj['Ementa'])
						#time.sleep(5)
						
				
			except Exception as e:
				#logging.error('[DEBUG] - Major problem processing the following text pair: Title: %s --- Text: %s' %(title, texto))
				print('[DEBUG] - Major problem processing the following text pair: Title: %s --- Text: %s' %(title, texto))
				print(e)
		
		### 2020-08-18 - New policy, we write out the JSON results line by line and we give it a timestamp too.
		dj['Timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
		json_to_write = "%s,\r\n" %(json.dumps(dj))
			
		jsonf = open('tjsp_json_data.json', 'a')
		jsonf.write(json_to_write)
		jsonf.flush()
		jsonf.close()
		
		#
		# Agora vamos fazer um rapido conversao de JSON pra SQL e inserir no banco de dados sqlite3
		# Premeiro vamos ter que mudar as datas extraidas do site para ISO format
		#
		tdate = dj['Data do Julgamento']
		tdate_parts = tdate.split('/')
		tdate_parts.reverse()
		tdate = '-'.join(tdate_parts)
		dj['Data do Julgamento'] = tdate;
	
		pdate = dj['Data da Publicacao']
		pdate_parts = pdate.split('/')
		pdate_parts.reverse()
		pdate = '-'.join(pdate_parts)
		dj['Data da Publicacao'] = pdate
		
		print("\n[+] Saving to SQL\n")
		# Agora joga pro library function json_to_sqlite_single() (single quer dizer que trata com um entry so)
		json_to_sqlite_single('tjsp_robtest.db', dj)
		
		
	# Now we're going to overwrite our session file with our current *PAGE* number... that way we can recover like Ninjas if we ever need to
	# TJSP is definitely page based
	page_num += 1
	sfhandle = open('session_page_number', 'w')
	sfhandle.write("%d\n" %(page_num))
	sfhandle.flush()
	sfhandle.close()
	
	# Esta na hora de pular pro proxima pagina dos resultados da busca...
	
	
	#
	# Esse codigo em baixo e' o "Jeito velho" de fazer as coisas: Usa muito 
	# Selenium inves de fazer uso de Python Requests e Beautiful Soup como
	# a gente queria.
	#
	# Porem, como podem ver mais em baixo, o "Jeito Certo" simplesmente nao
	# esta funcionando no momento. Como esse crawler tem um Roadblock 
	# muito mais complicado (da erro depois de processar uns 20,000 acoes)
	# e como esse 'hack' esta sim funcionado eu estou, por enquanto, deixando
	# quieto.
	#
	
	temp_table = None
	
	logging.info("Retrieving next page of results manually.....")	
	for retries in range(5):
		driver.get('https://esaj.tjsp.jus.br/cjsg/trocaDePagina.do?tipoDeDecisao=A&pagina=%d&conversationId=' %(page_num))
		html_data = driver.page_source
		temp_soup = BeautifulSoup(html_data, 'html.parser')
		temp_table = temp_soup.find('table')
		if temp_table == None:
			sleep_time = 5 * (retries+1)
			# Na verdade a gente nao *sabe* se e' um 403....
			# Selenium nao informa esse tipo de informacao porem 
			# do que deduzi usando Burp Suite isso e' a opcao
			# mais provavel....
			logging.error("TJSP threw us a 403... we'll wait %d seconds and try again" %(sleep_time))
			time.sleep(sleep_time)
			continue
		break
			
	if temp_table == None:
		logging.error("After 5 retries giving us a massive wait TJSP is still sending us 403s.\nThe only decent thing to do is close up and let the watchdog deal with us\n")
		driver.quit()
		sys.exit()
		
	logging.info("Got new page, continuing on!")



		
	# 
	# O codigo em baixo e' o verdadeiro Jeito Certo. Porem, no momento o
	# conteudo de r.text nao esta igual a contuedo do driver.page_source
	# fornecido do Selenium#
	# O efeito disso e' que quando a gente tenta parsar o HTML dar em um
	# bocado de erro. 
	#
	# Quando eu tiver mais tempo eu vou fazer um analise mais profunda em
	# o que tem extamente em r.text e o pq de nao esta batendo com o 
	# driver.page_source tirado do mesmo url. Pode ser coisa de frame, de
	# iframe, de Javascript fazendo post-load-rendering.... mas em tudo
	# jeito o ideal e' nao fica dependendo do driver.get() pq:
	#
	# driver.get() sucks because:
	# 	- really crappy timeout handling
	#	- really crappy handling of HTTP codes 
	#	- Generally works great for automating the browser and doing things that absolutely positively require JS exec / rendering
	#	- Generally crappy at everything else
	#	- So we want to use driver *ONLY* when we need it and requests when we don't 
	

	"""
	request_cookies_browser = driver.get_cookies()
		
	for c in request_cookies_browser:
		s.cookies.set(c['name'], c['value'])
		
	for retries in range(5):	
		r = s.get('https://esaj.tjsp.jus.br/cjsg/trocaDePagina.do?tipoDeDecisao=A&pagina=%d&conversationId=' %(page_num))
		if r.status_code == 200:
			break
		elif r.status_code == 403:
			print("TJSP hit us with a 403 code... waiting 5 seconds")
			time.sleep(5)
			continue
		else:
			print("TJSP hit us with a %d code... (huh?) waiting 5 seconds", r.status_code)
			time.sleep(5)
			continue			
			
	if r.status_code != 200:
		print("After 5 retries giving us a total of 25 seconds of wait TJSP is still sending us 403s.\nThe only decent thing to do is close up and let the watchdog deal with us\n")
		driver.quit()
		sys.exit()
	
	html_data = r.text <-- se a gente continua desde ca a gente bate no if table == None: --> logging.error("[!!!] A theoretically impossible error has occured.")
	                       Isso indica que o HTML no r.text nao esta o que estamos esperando / o que temos no driver.page_source. 
	"""
	


