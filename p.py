import json
import dpath
from typing import List, Dict
import pandas as pd
import math
import matplotlib.pyplot as plt
import os

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

class Token:
    def __init__(self, rectangle:List, symbols:List, confidence:float) -> None:
        self.rectangle = rectangle
        self.symbols = symbols
        self.confidence = confidence
        self.left = self.rectangle[0]
        self.top = 1-self.rectangle[1]
        self.right = self.rectangle[2]
        self.bottom = 1-self.rectangle[3]
    
    @property
    def text(self) -> str:
        return ''.join(char.get('text',' ') for char in self.symbols)

    @property
    def height(self) -> float:
        return self.top-self.bottom
    
    @property
    def length(self) -> float:
        return self.right-self.left

    def __repr__(self):
        return f'Y:{self.top:.3f} X:{self.left:.3f} ---- "{self.text}" ({self.confidence:.02f})'

class Tokens:
    
    def __init__(self, tokens: list) -> None:
        self.tokens=tokens

    truncate = 3
    epsilon = 0.007
    word_gap = 0.01008403999999996


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

    @property
    def sorted_tokens(self) -> list:
        return sorted(
            self.tokens,
            key=lambda token: (-token.top, token.left),
        )

    def truncate_values(self, values: list, truncate: int):
        return [round(value, truncate) for value in values]

    def get_all_values(self, attribute: str):
        assert attribute in ['top', 'left', 'right', 'bottom'], 'Invalid attribute name'
        return [getattr(token, attribute) for token in self.tokens]
    
    @property
    def rows(self):
        new_row, rows = [], []
        prev_y = math.inf
        for token in self.sorted_tokens:
            is_new_row = bool(abs(token.top - prev_y) > self.epsilon)
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
        # print(rows)
        return rows
    
    
    @property
    def gaps(self):
        gaps = []
        temp_interval, intervals = self.intervals[0], self.intervals[1:]
        for index, interval in enumerate(intervals):
            if index and not interval.overlaps(temp_interval):
                gaps.append(pd.Interval(left=temp_interval.right, right=interval.left))
                temp_interval = interval
            if interval.right > temp_interval.right:
                temp_interval=pd.Interval(temp_interval.left, interval.right)
        return gaps

    def _filter_gaps(self, gap_threshold=0.6):
        gaps = [gap for gap in self.gaps if gap.length >= self.word_gap * gap_threshold]
        return gaps if gaps else self.gaps

    @property
    def intervals(self):
        intervals = []
        HEADER_HEIGHT=0.15
        MAX_OFFSET = 0.2
        for row in self.rows:
            for token in row:
                if token.height < HEADER_HEIGHT and row[0].left < MAX_OFFSET:
                    # print(row)
                    # print(token)
                    # print(token.left, token.right)
                    try:
                        intervals.append(pd.Interval(token.left, token.right))
                    except ValueError:
                        intervals.append(pd.Interval(token.right, token.left))
        intervals.sort(key=lambda interval: (interval.left, interval.right))
        return intervals

    @property
    def columns(self):
        limits = [gap.left for gap in self._filter_gaps()] + [1]
        for token in self.tokens:
            for index, limit in zip(range(len(limits), 0, -1), limits[::-1]):
                if token.left <= limit:
                    token.column=index
        columns = []
        for index in range(1, len(limits) + 1):
            columns.append([token for token in tokens if token.column==index])
        return columns

    def plot_intervals(self):
        df = pd.DataFrame( 
            {'left':[i.left for i in self.intervals],
            'right':[i.right for i in self.intervals]},
            )
        start, end = df['right'], df['left']
        width = end-start
        fig, ax = plt.subplots()
        ax.barh(width=width, left=start, height=0.005, y=df.index, color='red',edgecolor='red')
        for gap in self.gaps:
            ax.bar(height=len(self), alpha=0.5, x=gap.left, width=gap.right-gap.left, color='blue', align='edge')
        plt.show()

    def get_tables(self, table_threshold=0.12):
        tables, new_table = [], []
        for index, row in enumerate(self.rows):
            large_gap = index and row[0].top - self.rows[index-1][0].top >= table_threshold
            if large_gap:
                tables.append(new_table)
                new_table = []
            new_table.extend(row)
        tables.append(new_table)
        return [Table(table) for table in tables]

   
class Table(Tokens):

    def get_dataframe(self):
        cols = {index:list() for index in range(1, len(tokens._filter_gaps()) + 2)}
        for row in self.rows:
            text = ''
            for col in cols:
                self.columns
                text = ' '.join(token.text for token in row if token.column==col)
                cols[col].append(text)
        return pd.DataFrame(cols)
    
    def save_csv(self, address:str):
        self.get_dataframe().to_csv(address)

def get_pages_from_file(results):
    pages = dpath.search(results, '/responses/*/fullTextAnnotation/pages', yielded=True)
    return list(pages)
    

for file in ['sal_sp.jsonoutput-1-to-1.json']:
    print(json)
    for address, page in get_pages_from_file(open_results_file(f"./{file}")):
        tokens = get_tokens_from_words(get_words_from_results(page))
        tokens = Tokens([token for token in tokens if token.top < 0.6])
        for n, table in enumerate(tokens.get_tables()):
            print(n)
            # try:
            table.save_csv(f"{file}_{address.replace('/', '_')}_{n}")
            # except Exception as e:
            #     rai
    
