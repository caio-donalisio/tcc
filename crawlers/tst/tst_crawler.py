#!/usr/bin/env python3

import json
import requests
import time
import os
import sys
from datetime import datetime
import re

sys.path.append('../libs/')

from json_to_sqlite import json_to_sqlite_single, sqlite_increment_dupcount

def download_pdf_file(teor_link, pdf_file_name):

	r = ''
	
	for retries in range(5):
		try:	
			print("Trying to GET %s" %(teor_link))
			r = requests.get(teor_link, timeout=(2*(retries+1)))
			print("Out of GET")
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
		print("Unable to access first_stage_url after 5 tries. Letting watchdog take care of us.")
		#driver.quit()
		return False

	f = open(pdf_file_name, "wb")
	f.write(r.content)
	f.flush()
	f.close()
	
		
	return True

"""
dj = {'Tipo Processual': None, 'NumAcordao':None, 'NumProcesso':None, 'Relator(a)': None, 'Orgao Julgador':None, 'Data do Julgamento':None, 'Classe/Assunto':None, 'Requerente':None, 'Requerido':None, 'PathToPdf':None}


			dj['Tipo Processual'] = acordao_text.split(' - ')[0]
			dj['NumAcordao'] = acordao_text.replace(' ', '')
			pdf_filename = "downloaded_pdfs/%s.pdf" %(dj['NumAcordao'])
			dj['PathToPdf'] = pdf_filename
			
			dj['Ementa'] = ''
			dj['Orgao Julgador'] = parts[1]
			dj['Relator(a)'] = parts[1]
			dj['Data do Julgamento']
			dj['Data da Publicacao']
			dj['Timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
			
"""

def process_page(page_num):
	post_dict = {"ou":None,"e":"para","termoExato":"","naoContem":None,"ementa":None,"dispositivo":None,"numeracaoUnica":{"numero":None,"digito":None,"ano":None,"orgao":"5","tribunal":None,"vara":None},"orgaosJudicantes":[],"ministros":[],"convocados":[],"classesProcessuais":[],"indicadores":[],"tiposDecisoes":[],"tipos":["ACORDAO"],"orgao":"TST"}

	dj = {'Tipo Processual': None, 'NumAcordao':None, 'NumProcesso':None, 'Relator(a)': None, 'Orgao Julgador':None, 'Data do Julgamento':None, 'Classe/Assunto':None, 'Requerente':None, 'Requerido':None, 'PathToPdf':None}

	r = requests.post("https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual/%d/20" %(page_num), json.dumps(post_dict), headers={'Content-Type':'application/json'})

	#print(r.text)
	#print(r.content)

	try:
		resp = json.loads(r.content)
	except:
		dst = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
		debug_basefilename = "debug-%s" %(dst)
		
		debug_meta_filename = debug_basefilename + ".meta"
		debug_content_filename = debug_basefilename + ".content"
		debug_text_filename = debug_basefilename + ".text"
		
		print("[-] Exception while trying to write out r.content - dumping out to %s.xxx" %(debug_basefilename))
		
		debug_f = open(debug_meta_filename, "w")
		for header in r.headers:
			debug_f.write(header + ': ' + r.headers[header])
			
		debug_f.flush()
		debug_f.close()
		
		debug_f = open(debug_content_filename, "w")
		debug_f.write(str(r.content))
		debug_f.flush()
		debug_f.close()
		
		debug_f = open(debug_text_filename, "w")
		debug_f.write(r.text)
		debug_f.flush()
		debug_f.close()
		
		return False # -xxx- Should we quit here? I mean we are definitely losing 20 entries .... but then it's less to recrawl I guess IDK....
		
	if not 'registros' in resp:
		print("[---] Error: Badly formed JSON caught early - the entire JSON response is bad - returning False here... exiting might be correct but we can change that later")
		dst = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
		debug_filename = "json-error1-debug-%s.json" %(dst)
		debug_f = open(debug_filename, "w")
		debug_f.write(r.content)
		debug_f.flush()
		debug_f.close()
		print("[---] Have written the bad JSON to %s\n\n" %(debug_filename))

		return False
		
	for reg in resp['registros']:
	
		try:
			registro=reg['registro']
	
			num_formatado = registro['numFormatado']
			data_julgamento = registro['dtaJulgamento']
			data_publicacao = registro['dtaPublicacao']
			nome_relator = registro['nomRelator']
	
			oj = registro['orgaoJudicante']
	
			oj_str = oj['descricao']
	
			#print(num_formatado)
			#print(data_julgamento)
			#print(data_publicacao)
			#print(oj_str)
			#print(nome_relator)
		

			#http://aplicacao5.tst.jus.br/consultaDocumento/acordao.do?anoProcInt=2020&numProcInt=147033&dtaPublicacaoStr=22/01/2021%2007:00:00&nia=7585700
	
			anoProcInt = int(registro['anoProcInt'])
			numProcInt = int(registro['numProcInt'])
			dtaPublicacaoStr = registro['dtaPublicacao'] #2021-01-22T00:00:00-03
			dtaPublicacaoStr = dtaPublicacaoStr.split('T')[0]
			dtaPublicacaoStr_parts = dtaPublicacaoStr.split('-')
			dtaPublicacaoStr_parts.reverse()
			dtaPublicacaoStr = '/'.join(dtaPublicacaoStr_parts)
			dtaPublicacaoStr = "%s%%2007:00:00" %(dtaPublicacaoStr)
	
			nia = int(registro['numInterno'])
	
			download_pdf_url = "http://aplicacao5.tst.jus.br/consultaDocumento/acordao.do?anoProcInt=%d&numProcInt=%d&dtaPublicacaoStr=%s&nia=%d" %(anoProcInt, numProcInt, dtaPublicacaoStr, nia)
			pdf_file_name = "downloaded_pdfs/%s.pdf" %(num_formatado.replace(' ', ''))
	
			print(download_pdf_url)
		
			acordao_text = num_formatado
		
			dj['Tipo Processual'] = acordao_text.split(' - ')[0]
			dj['NumAcordao'] = acordao_text.replace(' ', '')
			pdf_filename = "downloaded_pdfs/%s.pdf" %(dj['NumAcordao'])
			dj['PathToPdf'] = pdf_filename
			
			ementa_text = registro['ementa']
			ementa_text = re.sub("\s\s+", " ", ementa_text)
			dj['Ementa'] = ementa_text
			dj['Orgao Julgador'] = oj_str
			dj['Relator(a)'] = nome_relator
			dj['Data do Julgamento'] =  registro['dtaJulgamento'].split('T')[0]
			dj['Data da Publicacao'] =  registro['dtaPublicacao'].split('T')[0]
		
			dj['Timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
		except Exception as e:
			print("[---] Error while processing a JSON reg['registro'] entry")
			print("[---] I'm going to dump the entire JSON response because it's 9pm and I'm too tired to figure out how to re-stringify only this 'registro', but maybe I'll figure that out tomorrow")
			dst = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
			debug_filename = "json-error2-debug-%s.json" %(dst)
			debug_f = open(debug_filename, "w")
			debug_f.write(str(r.content))
			debug_f.flush()
			debug_f.close()
			print("[---] Have written the bad JSON to %s, will now dump Exception error message and return False (should we just exit here?):" %(debug_filename))
			print(e)
			print("\n\n")
			
			return False
			
		#print(dj)
		#time.sleep(5)
		if os.path.isfile(pdf_file_name):
			print("[-] DUPLICATE")
			sqlite_increment_dupcount('tst.db', dj['NumAcordao'])
		else:
			res = download_pdf_file(download_pdf_url, pdf_file_name)
			if not res:
				print("Unable to download PDF file exit....")
				sys.exit()
			else:
				print("[+] Downloaded PDF OK")
				json_to_sqlite_single('tst.db', dj)
		
	
	
	
		print("\n\n")
	
	
if __name__ == "__main__":
	
	if os.path.isfile("tst_document_num"):
		print("[+++] Found session file")
		document_num_f = open('tst_document_num', 'r')
		page_num = int(document_num_f.read())
		document_num_f.close()
		print("[+++] Starting from document %d" %(page_num))
	else:
		print("[+++] Starting from document 1")
		page_num = 1
		
	while True:
		process_page(page_num)
		#
		# I feel like if process_page() returns False then we should error out here.
		# If we fail to process an entire page (or a subset of that page) then the below counter is incremented but we didn't process those entries.
		# I feel like that is a Bad Thing. 
		#
		# Slightly unrelated but I also feel like we should have a way to signal to the Watchdog "do not restart me, bro" in certain cases when we exit
		#
		page_num += 20
		document_num_f = open('tst_document_num', 'w')
		document_num_f.write("%d\n" %(page_num))
		document_num_f.flush()
		document_num_f.close()
		
		
	




