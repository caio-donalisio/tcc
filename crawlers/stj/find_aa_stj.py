import os

"""

This extracts AGRAVADO and AGRAVANTE fields from PDFs from STJ

It will soft fail if:

1) The PDF download or save failed
2) The PDF is not the correct format
3) We are looking for a different 'pair' such as IMPETRANTE/PACIENTE or SUSCITANTE/SUSCITADO
4) It will only fail on the above for 99% of the PDFs ;p, 'ITA-style' PDFs will parse just fine because of the different processing method!

It should not hard fail (crash) on any inputs

"""


# This routine is for 'ITA-style' PDFs (I have no idea what that means but there is a different GET var 'ITA' and the formatting of the text in the PDF
# is totally different. At the end of the day it's 1000x easier (at this point) to just work around and get things working than to wonder why....
def get_aa_from_stj_ita_pdf(filename):
	pdf_to_text_cmd = "pdftotext %s" %(filename)
	os.system(pdf_to_text_cmd) # shortcut - use pdftotext to get plain text"

	textfile = filename.replace('.pdf', '.txt')
	
	try:
		f = open(textfile, "r")
	except:
		print("[--] Failed to open a textfile, going to have to assume that pdf download / conversion failed")
		return None, None
		
	lines = f.readlines()

	tipo_de_processo = lines[1].strip()

	lines = lines[2:]

	wanted_lines=[]

	for line in lines:
		if line.strip() == ':' or line.strip() == '':
			continue
		if line.startswith('EMENTA'):
			break
		if not line.startswith('ADVOGA') and line.count(' - ') == 0: # We don't want to include advogado(a)s in our data set for this purpose
			wanted_lines.append(line.strip())
	
	try:
		relator = wanted_lines[4]
		agravante = wanted_lines[5]
		agravado = wanted_lines[6]
	except Exception as e:
		print('[-] Unable to parse this PDF adequately.... erroring out in the nicest way possible')
		print('[-] Exception: %s' %(e))
		f.close()
		os.unlink(textfile)
		return None, None

	"""	
	print("Tipo de processo: %s" %(tipo_de_processo))
	print("%s -> %s" %(wanted_lines[0], wanted_lines[4]))
	print("%s -> %s" %(wanted_lines[1], wanted_lines[5]))
	print("%s -> %s" %(wanted_lines[2], wanted_lines[6]))	
	"""
	
	f.close()
	os.unlink(textfile) # clean up after ourselves
		
	return agravado, agravante # return a tuple

def get_aa_from_stj_pdf(filename):

	pdf_to_text_cmd = "pdftotext %s" %(filename)
	os.system(pdf_to_text_cmd) # shortcut - use pdftotext to get plain text"

	textfile = filename.replace('.pdf', '.txt')
	
	try:
		f = open(textfile, "r")
	except:
		print("[--] Failed to open a textfile, going to have to assume that pdf download / conversion failed")
		return None, None

	agravado = None
	agravante = None

	while(True):
		if agravante and agravado:
			break

		line = f.readline()
		if line == "": # We've reached the end of the file
			break
    
		line = line.strip()

		if line == 'AGRAVANTE':
			t = f.readline() # If line == 'AGRAVANTE' then next line is that person's name!
			agravante = t.strip(':')
			agravante = agravante.strip()
			#print("Agravante: %s" %(agravante))
			continue

		if line == 'AGRAVADO': # If line == 'AGRAVADO' then next line is that person's name!
			t = f.readline()
			agravado = t.strip(':')
			agravado = agravado.strip()
			#print("Agravado: %s" %(agravado))
			continue

	f.close()
	os.unlink(textfile) # clean up after ourselves
		
	return agravado, agravante # return a tuple
