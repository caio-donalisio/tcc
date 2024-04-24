from utils import get_pages_from_file, open_results_file, get_words_from_results, get_tokens_from_words
from models import TokenSet
from glob import glob
from config import FILES_DIR
from columns import TableInferer

if __name__ == '__main__':
    for file in glob(f'{FILES_DIR}/*COMPLETE.json'):
        print('Processing: ', file)
        for page_number, (page, metadata) in enumerate(get_pages_from_file(file), start=1):
            if page_number >= 5:
                print('Extracting page -', page_number)
                tokens = TokenSet(tokens = get_tokens_from_words(get_words_from_results(page)),
                                    metadata = metadata)
                tokens.columns
                # for n, table in enumerate(tokens.get_tables()):
                #     ...
                # print(n)
                # try:
                # table.save_csv(f"{file}_{address.replace('/', '_')}_{n}")
                # except Exception as e:
                #     rai
        