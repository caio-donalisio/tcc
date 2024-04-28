import json
from typing import Dict, List
from models import Token
import dpath
from glob import glob
from config import FILES_DIR
from copy import deepcopy
import requests
import os

def open_results_file(filepath: str):
    with open(filepath) as f:
        data = json.loads(f.read())
    return data

def get_words_from_results(data: Dict):
    words = []
    for _, word in dpath.search(
        data,
        'pages/*/blocks/*/paragraphs/*/words',yielded=True):
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
    return tokens

def flatten_list(l: list):
    return [x for y in l for x in y]

def join_all_pages(prefix: str):
    pages = []
    for file in sorted(glob(f'{FILES_DIR}/{prefix}*'), 
                       key=lambda filename: int(filename.split('-')[1])):
        data = open_results_file(file)
        pages.extend(sorted(
            data['responses'], 
            key= lambda response: response['context']['pageNumber']))
    assert [page['context']['pageNumber'] for page in pages] == list(range(1, len(pages) + 1)), 'Missing page'
    with open(f'{FILES_DIR}/{prefix}_COMPLETE.json', 'w') as f:
        f.write(json.dumps(pages))

def get_pages_from_file(filepath: str):
    blank_page = {'pages': None}
    for page in open_results_file(filepath):
        text_annotation = page.get('fullTextAnnotation', blank_page)
        metadata = text_annotation['pages'][0]
        pages = deepcopy(text_annotation)
        if metadata.get('blocks'):
            del metadata['blocks']
        metadata = {**page.get('context', {}), **metadata}
        yield pages, metadata

def get_google_vision_response(image_path: str):
    api_key = os.environ['GOOGLE_VISION_API_KEY']
    url = f'https://vision.googleapis.com/v1/images:annotate?key={api_key}' + api_key

    with open(image_path, 'rb') as image_file:
        image_content = image_file.read()

    payload = {
        'requests': [
            {
                'image': {
                    'content': image_content
                },
                'features': [
                    {
                        'type': 'DOCUMENT_TEXT_DETECTION'
                    }
                ]
            }
        ]
    }

    response = requests.post(url, json=payload)
    response_data = response.json()

    return response_data