import json
import sqlite3

# The string representing the json.
# You will probably want to read this string in from
# a file rather than hardcoding it.
f = open('stj_json_data.json', 'rb')
s = f.read()
f.close()

s = s.decode('utf-8')
# Read the string representing json
# Into a python list of dicts.
data = json.loads(s)

json_to_sqlite(data)

def json_to_sqlite(data):
"""
{
"Tipo Processual": "AgRg", 
 "NumAcordao": "202000314865", 
 "NumProcesso": "AgRg no RHC 123770 ", 
 "Relator(a)": "Ministro FELIX FISCHER (1109)", 
 "Orgao Julgador": "Superior Tribunal de Justi\u00c3\u00a7a - T5 - QUINTA TURMA",
 "Data da Publicacao": "DJe 08/09/2020", 
 "Data do Julgamento": "08/09/2020", 
 "Classe/Assunto": None, 
 "Requerente": "ADVOGADO", 
 "Requerido": "", varchar(100)
 "PathToPdf": "downloaded_pdfs/202000314865.pdf",
 "Timestamp": "2020-09-21 00:35:11.960"
 }
"""
# Open the file containing the SQL database.
with sqlite3.connect("stj.db") as conn:

    # Create the table if it doesn't exist.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS tab(
		id INTEGER PRIMARY KEY,
		TipoProcessual text,
                NumAcordao text,
                NumProcesso text,
		Relator_a text,
		OrgaoJulgador text,
		DataDoJulgamento DATE,
		DataPublicacao DATE,
		Classe_Assunto text,
		Requerente text,
		Requerido text,
		Ementa text,
		PathToPdf text,
		Timestamp DATETIME
            );"""
        )

    # Insert each entry from json into the table.
    #keys = ["id", "name", "age", "salary"]
    keys = ["Tipo Processual", "NumAcordao", "NumProcesso", "Relator(a)", "Orgao Julgador", "Data do Julgamento", "Data da Publicacao", "Classe/Assunto", "Requerente", "Requerido", "Ementa", "PathToPdf", "Timestamp"]
    
    for entry in data:
        tdate = entry['Data do Julgamento']
        parts = tdate.split('/')
        parts.reverse()
        tdate = '-'.join(parts)
        entry['Data do Julgamento'] = tdate;
	
        pdate = entry['Data da Publicacao']
        pdate = pdate.split(' ')[1]
        parts = pdate.split('/')
        parts.reverse()
        pdate = '-'.join(parts)
        entry['Data da Publicacao'] = pdate
	
        # This will make sure that each key will default to None
        # if the key doesn't exist in the json entry.
        values = [None] + [entry.get(key, None) for key in keys]

        # Execute the command and replace '?' with the each value
        # in 'values'. DO NOT build a string and replace manually.
        # the sqlite3 library will handle non safe strings by doing this.
        cmd = """INSERT INTO tab VALUES(
                    ?,
                    ?,
		    ?,
                    ?,
                    ?,
                    ?,
		    ?,
		    ?,
		    ?,
                    ?,
                    ?,
		    ?,
		    ?,
                    ?
                );"""
        conn.execute(cmd, values)

    conn.commit()
    
