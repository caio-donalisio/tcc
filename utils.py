import json

def open_results_file(filepath: str):
    with open(filepath) as f:
        data = json.loads(f.read())
    return data

def get_words_from_results(data: Dict):
    words = []
    for _, word in dpath.search(
        data,
        '*/blocks/*/paragraphs/*/words',yielded=True):
        words.append(word)
    return words

def get_tokens_from_words(results: List):
    tokens = []
    for words in results:
        for word in words:
            verts = word['boundingBox']['normalizedVertices']
            try:
                left, top = verts[0]['x'], verts[0]['y']
                right, bottom = verts[2]['x'], verts[2]['y']
            except KeyError:
                left, top, bottom, right=0,0,0,0
            rect = [left,top,right,bottom]
            tokens.append(Token(rect, word['symbols'], word['confidence']))
    return Tokens(tokens)

def flatten_list(l: list):
    return [x for y in l for x in y]