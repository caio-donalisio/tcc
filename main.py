import utils
from models import TokenSet, Table
from glob import glob
import config
import os
from pathlib import Path
import logging
import sys

logger = logging.getLogger("table_generator")
logger.addHandler(logging.StreamHandler(sys.stdout))
logging.basicConfig(
    filename='table_generator.log', 
    encoding='utf-8', 
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

if __name__ == '__main__':
    
    #DESKEWING
    for file in glob(f'{config.ORIGINAL_FILES_DIR}/*.pdf'):
        path = Path(file)
        deskewed_path = Path(f"{config.DESKEWED_FILES_DIR}/{path.name}")
        if not deskewed_path.exists():
            logger.info(f'Deskewing: {path.name}')
            os.system(f'sudo docker run --rm -i jbarlow83/ocrmypdf-alpine - - -d -s <"{file}" >"{deskewed_path.absolute().__str__()}"')
        else:
            logger.info(f"DESKEWED {path.name} already present, skipping step...")
    
    #GET GOOGLE VISION RESPONSE
        complete_ocr_path = Path(f"{config.JOINED_OCRED_DIR}/{path.with_suffix('').name}.json")
        # utils.join_all_pages(path.with_suffix('').name)
        if not complete_ocr_path.exists():
            logger.info(f'Getting Google Vision response: {path.name}')
            utils.get_google_vision_response(deskewed_path)
            utils.join_all_pages(path.with_suffix('').name)
        else:
            logger.info(f"GOOGLE VISION {path.name} already present, skipping step...")

    #GET TABLES
        logger.info(f'Generating table: {path.name}')
        for page_number, (page, metadata) in enumerate(utils.get_pages_from_file(complete_ocr_path), start=1):
            if page_number > 50: continue
            logger.info(f'Extracting page - {page_number:04}')
            try:
                if metadata:
                    table = Table(tokens = utils.get_tokens_from_words(utils.get_words_from_results(page)),
                                metadata = metadata)
                else:
                    raise IndexError('No metadata found')
            except IndexError as e:
                logger.error(e)
            table.save_csv()# run()