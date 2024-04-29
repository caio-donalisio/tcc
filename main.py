from utils import get_pages_from_file, get_words_from_results, get_tokens_from_words
from models import TokenSet
from glob import glob
import config
from columns import TableInferer
import os
from pathlib import Path


if __name__ == '__main__':
    
    # for file in glob(f'{config.ORIGINAL_FILES_DIR}/*.pdf'):
    #     path = Path(file)
    #     deskewed_path = Path(f"{config.DESKEWED_FILES_DIR}/{path.name}")
    #     if not deskewed_path.exists():
    #         print('Deskewing: ', file)
    #         os.system(f'sudo docker run --rm -i jbarlow83/ocrmypdf-alpine - - -d -s <"{file}" >"{deskewed_path.absolute().__str__()}"')
    #     else:
    #         print(f"{file=} already present, skipping deskewing...")
    
    
    
    
    
    for file in glob(f'{config.JOINED_OCRED_DIR}/*COMPLETE.json'):
        print('Processing: ', file)
        for page_number, (page, metadata) in enumerate(get_pages_from_file(file), start=1):
            if page_number >= 5:
                print('Extracting page -', page_number)
                tokens = TokenSet(tokens = get_tokens_from_words(get_words_from_results(page)),
                                    metadata = metadata)
                tokens.columns
        