import os
import logging

def tjsp_parse_pdf(filename):
	requeridos = ''
	requerentes = ''

	pdf_to_text_cmd = "pdftotext %s" %(filename)
	os.system(pdf_to_text_cmd)
	
	textfile = filename.replace('.pdf', '.txt')
	
	try:
		f = open(textfile, "r")
	except:
		print("[--] Failed to open a textfile, going to have to assume that pdf download / conversion failed")
		return None, None
	
	#print("Opened textfile %s" %(textfile))
	
	while True:
		if f.tell() == os.fstat(f.fileno()).st_size:
			#print('EOF - quit2')
			break
			
		line = f.readline()
		if line == '':
			#print('EOF - quit')
			break

		if line.startswith('FORO DE ORIGEM'):
			requeridos = f.readline().split(':')[1].strip()
			while True:
				if f.tell() == os.fstat(f.fileno()).st_size:
					print('EOF - quit2')
					break
				line2 = f.readline()
				if line2.count(':') > 0:
					requerentes += line2.split(':')[1].strip()
					break
				else:
					requeridos += ", %s" %(line2.strip())
			
			while True:
				if f.tell() == os.fstat(f.fileno()).st_size:
					print('EOF - quit2')
					break
				line2 = f.readline()
				if line2.startswith('COMPET'):
					break
				else:
					requerentes += ", %s" %(line2.strip())
		else:
			continue
	
		break

	os.unlink(textfile)
	
	return requerentes,requeridos

"""
JUIZ(a) PROLATOR(a) DA SENTENÇA: Sandro Rafael Barbosa Pacheco
APELANTE: Ministério Público
APELADOS: Kleiton Rodrigues Melo e Jhonata Francisco Anastácio

^ Have to deal with this awfulness too!
"""


