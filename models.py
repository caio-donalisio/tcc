import functools
import pandas as pd
import math
from typing import List
from dataclasses import dataclass
import string
from collections import defaultdict
import logging
from columns import TableInferer
from pathlib import Path
from config import GAP_BETWEEN_LINES, DESKEWED_FILES_DIR, OUTPUT_TABLES_FILES_DIR

logger = logging.getLogger("table_generator")

@dataclass
class Point:
    x: float
    y: float
        
    def __repr__(self):
        return f'X:{self.x:.3f} Y:{self.y:.3f}'

class Token:
    def __init__(self, rectangle:List, symbols:List, confidence:float) -> None:
        self.rectangle = rectangle
        self.symbols = symbols
        self.confidence = confidence
        self.left = self.rectangle[0]
        self.top = 1-self.rectangle[1]
        self.right = self.rectangle[2]
        self.bottom = 1 - self.rectangle[3]
    
    @property
    def text(self) -> str:
        return ''.join(char.get('text',' ') for char in self.symbols)

    @property
    def x_center(self) -> float:
        return (self.left + self.right)/2

    @property
    def height(self) -> float:
        return self.top-self.bottom
    
    @property
    def length(self) -> float:
        return self.right-self.left

    @property
    def data_type(self) -> str:
        counts = defaultdict(int)
        for char in self.text:
            if char.isdigit():
                counts['number'] += 1
            elif char.isalpha():
                counts['text'] += 1
            elif char in string.punctuation:
                counts['punctuation'] += 1

        if counts['number'] >= counts['text'] and counts['number'] > counts['punctuation']:
            data_type = 'number'
        elif counts['text'] >= counts['number'] and counts['text'] > counts['punctuation']:
            data_type = 'text'
        else:
            data_type = 'punctuation'
        return data_type

    def __repr__(self):
        return f'Y:{self.top:.3f} X:{self.left:.3f} ---- "{self.text}" ({self.confidence:.02f})'

class TokenSet:
    
    def __init__(self, tokens: list, metadata: dict) -> None:
        self.tokens=tokens
        self.page_number = metadata.get('pageNumber')
        self.filepath = self.extract_filepath(metadata)
        self.width = metadata.get('width')
        self.height = metadata.get('height')

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index >= len(self.tokens):
            raise StopIteration
        value = self.tokens[self.index]
        self.index += 1
        return value

    def __len__(self):
        return len(self.tokens)
    
    def __getitem__(self, index):
        return self.tokens[index]

    def __repr__(self):
        return '\n'.join(str(token) for token in self.tokens)

    def extract_filepath(self, metadata):
        return Path(DESKEWED_FILES_DIR) / metadata.get('uri').split('/')[-1]

    @property
    def sorted_tokens(self) -> list:
        return sorted(
            self.tokens,
            key=lambda token: (-token.top, token.left),
        )

    # def truncate_values(self, values: list, truncate: int):
    #     return [round(value, truncate) for value in values]

    def _get_all_values(self, attribute: str):
        assert attribute in ['top', 'left', 'right', 'bottom'], 'Invalid attribute name'
        return [getattr(token, attribute) for token in self.tokens]
    
    @property
    def min_bottom(self):
        return min(self._get_all_values('bottom'))
    
    @property
    def max_top(self):
        return max(self._get_all_values('top'))
    
    @property
    def min_left(self):
        return min(self._get_all_values('left'))
    
    @property
    def max_right(self):
        return max(self._get_all_values('right'))
    
    # def max_text_position(self):
    #     max(token.left for token in self.tokens if token.data_type=='text')
    
    @property
    @functools.lru_cache()
    def rows(self):
        new_row, rows = [], []
        prev_y = math.inf
        for token in self.sorted_tokens:
            is_new_row = bool(abs(token.top - prev_y) > GAP_BETWEEN_LINES)
            if is_new_row:
                rows.append(new_row)
                new_row=[]
                prev_y = token.top
            new_row.append(token)
            new_row.sort(key=lambda token: token.left)
        rows.append(new_row)
        rows = list(filter(None, rows))
        for index, row in enumerate(rows):
            for token in row:
                token.row=index
        return rows
    
    @property
    @functools.lru_cache()
    def columns(self):
        inferer = TableInferer(self.filepath, self.page_number)
        return inferer.get_columns()
    
    def get_positions(self, index):
        for pack_index, column_pack in enumerate(self.columns, start=1):
            if pack_index != index: continue
            for row_index, row in enumerate(self.rows, start=1):
                for token in row:
                    token.row = row_index
                    for column_index, column_threshold in enumerate(column_pack, start=1):
                        if token.x_center <= column_threshold:
                        # if token.left <= column_threshold:
                            token.column = column_index
                            break
                    else:
                        token.column = len(column_pack) + 1
                    
    def get_dataframe(self):
        
        table = []
        dfs = []
        for pack_index, column_pack in enumerate(self.columns, start=1):
            self.get_positions(pack_index)
            for _, row in enumerate(self.rows, start=1):
                new_row = []
                for column_index in range(1, len(column_pack) + 2 ):
                    cell_tokens = [token for token in row if token.column==column_index]
                    if cell_tokens:
                        # min_confidence = min(token.confidence for token in cell_tokens)
                        text = ' '.join(token.text for token in cell_tokens)
                        new_row.append(text.split(':')[0])# + f' [[{min_confidence}]]')
                    else:
                        new_row.append('')
                table.append(new_row)
            dfs.append(pd.DataFrame(table))
        return dfs
                
class Table(TokenSet):

    def __init__(self, tokens: list, metadata: dict) -> None:
        super().__init__(tokens, metadata)
        self.dfs = self.get_dataframe()
    
    def save_csv(self, filename = None):
        for index, df in enumerate(self.dfs, start=1):
            filename = f"{self.filepath.with_suffix('').name}---{self.page_number:04}_{index:02}.csv"
            df.to_csv(
                Path(OUTPUT_TABLES_FILES_DIR) / filename,
                index=False)
        
    def run(self):
        ...
    