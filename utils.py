import json
from typing import Dict, List
from models import Token
import dpath
from glob import glob
import config
import os
from copy import deepcopy
from pathlib import Path
from google.cloud import vision

def open_results_file(filepath: str):
    with open(filepath, encoding="utf-8") as f:
        data = json.loads(f.read())
    return data

def get_words_from_results(data: Dict):
    words = []
    for _, word in dpath.search(
        data,
        '/blocks/*/paragraphs/*/words',yielded=True):
        words.append(word)
    return words

def get_tokens_from_words(results: List):
    tokens = []
    for words in results:
        for word in words:
            verts = word['boundingBox']['normalizedVertices']
            try:
                left, top = verts[0].get('x',0), verts[0]['y']
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
    for file in sorted(glob(f'{config.OCRED_PAGES_DIR}/{prefix}*'), 
                       key=lambda filename: int(filename.split('-')[1])):
        data = open_results_file(file)
        pages.extend(sorted(
            data['responses'], 
            key= lambda response: response['context']['pageNumber']))
    # assert [page['context']['pageNumber'] for page in pages] == list(range(1, len(pages) + 1)), 'Missing page'
    with open(f'{config.JOINED_OCRED_DIR}/{prefix}.json', 'w') as f:
        f.write(json.dumps(pages))

def get_pages_from_file(filepath: str):
    blank_page = {'pages': None}
    for page in open_results_file(filepath):
        text_annotation = page.get('fullTextAnnotation')
        if not text_annotation:
            yield blank_page, {}
            continue
        metadata = text_annotation['pages'][0]
        pages = deepcopy(text_annotation)['pages']
        if metadata.get('blocks'):
            del metadata['blocks']
        metadata = {**page.get('context', {}), **metadata}
        yield pages[0], metadata

def get_google_vision_response(filepath, batch_size=100):
    os.system(f'gsutil -m cp {filepath} gs://{config.BUCKET_NAME}')
    mime_type = "application/pdf"
    
    client = vision.ImageAnnotatorClient()
    
    
    
    feature = vision.Feature(type_=vision.Feature.Type.TEXT_DETECTION)
    gcs_source = vision.GcsSource(uri=f"gs://{config.BUCKET_NAME}/{filepath.name}")
    input_config = vision.InputConfig(gcs_source=gcs_source, mime_type=mime_type)
    gcs_destination = vision.GcsDestination(uri=f"gs://{config.BUCKET_NAME}/{filepath.with_suffix('.json').name}")
    
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size
    )
    
    text_detection_params = vision.TextDetectionParams(enable_text_detection_confidence_score=True)
    image_context = vision.ImageContext(text_detection_params=text_detection_params)
    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config, output_config=output_config, image_context=image_context
    )
    operation = client.async_batch_annotate_files(requests=[async_request])
    print("Waiting for the operation to finish.")
    operation.result(timeout=420)
    os.system(f'gsutil -m cp gs://{config.BUCKET_NAME}/{filepath.with_suffix(".json").name.replace(".json","*.json")} {config.OCRED_PAGES_DIR}')

# get_google_vision_response(Path(f'{config.DESKEWED_FILES_DIR}/teste9.pdf'))