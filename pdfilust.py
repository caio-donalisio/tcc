#!pip install pymupdf 
#!pip install dpath



#Lê o documento original
import fitz  
doc = fitz.open('./pag9.pdf')


#Lê o resultado do OCR
import json 
with open('pag9_resultado.json') as f:
    data = json.loads(f.read())

#Extrai as dimensões do documento
dims =  data['responses'][0]['fullTextAnnotation']['pages'][0]
WIDTH,HEIGHT = dims['width'], dims['height']
    
#Extrai as palavras do resultado do OCR
import dpath.util
results = []
for x,r in dpath.util.search(data,'responses/*/fullTextAnnotation/pages/*/blocks/*/paragraphs/*/words',yielded=True):
    results.append(r)


####################################################################################            
    
#Desenha retangulos no PDF com as palavras lidas correspondentes na posição correspondente
for page in doc:
    for n,words in enumerate(results):
        for word in words:
            verts = word['boundingBox']['normalizedVertices']
            x1, y1 = verts[0]['x'] * WIDTH, verts[0]['y'] * HEIGHT
            x2, y2 = verts[2]['x'] * WIDTH, verts[2]['y']  * HEIGHT
            rect = [x1,y1,x2,y2]
            text = ''.join(char.get('text',' ') for char in word['symbols'])
            page.add_freetext_annot(rect, text, fontsize=6.5,text_color=(1,0,0), fill_color=(1,1,1))

doc.save('./TEST_PAG9_PALAVRAS.pdf')

####################################################################################            

#Idem para "blocos"
results = []
for x,r in dpath.util.search(data,'responses/*/fullTextAnnotation/pages/*/blocks',yielded=True):
    results.append(r)
   
    
for page in doc:
    for n,blocks in enumerate(results):
        for block in blocks:
            verts = block['boundingBox']['normalizedVertices']
            x1, y1 = verts[0]['x'] * WIDTH, verts[0]['y'] * HEIGHT
            x2, y2 = verts[2]['x'] * WIDTH, verts[2]['y']  * HEIGHT
            rect = [x1,y1,x2,y2]
            page.add_freetext_annot(rect, '', fontsize=6.5, border_color=(0,1,0))
            
results = []


####################################################################################            

#Idem para "parágrafos"
for x,r in dpath.util.search(data,'responses/*/fullTextAnnotation/pages/*/blocks/*/paragraphs',yielded=True):
    results.append(r)
    

for page in doc:
    for n,blocks in enumerate(results):
        for block in blocks:
            verts = block['boundingBox']['normalizedVertices']
            x1, y1 = verts[0]['x'] * WIDTH, verts[0]['y'] * HEIGHT
            x2, y2 = verts[2]['x'] * WIDTH, verts[2]['y']  * HEIGHT
            rect = [x1,y1,x2,y2]
            page.add_freetext_annot(rect, '', fontsize=6.5, border_color=(0,0,1))

####################################################################################            
            
doc.save('./TEST_PAG9_ALL_OBJS.pdf')