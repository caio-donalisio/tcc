
import concurrent.futures
import pandas as pd
from matplotlib import pyplot as plt
import math
from typing import List
from dataclasses import dataclass
import string
from collections import defaultdict
import numpy
import concurrent
import PyPDF2
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import io
import numpy
from columns import TableInferer
from pathlib import Path

PDF_FILES_DIR = '.'

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

    # truncate = 3 # TRUNCATE DECIMALS
    epsilon = 0.007 # GAP BETWEEN LINES
    # word_gap = 0.01008403999999996 # GAP BETWEEN WORDS
    # grid_length = 0.002


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
        return Path(PDF_FILES_DIR) / metadata.get('uri').split('/')[-1]

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
    def min_bottom(self):
        return min(self.get_all_values('bottom'))
    
    @property
    def max_top(self):
        return max(self.get_all_values('top'))
    
    @property
    def min_left(self):
        return min(self.get_all_values('left'))
    
    @property
    def max_right(self):
        return max(self.get_all_values('right'))
    
    def max_text_position(self):
        max(token.left for token in self.tokens if token.data_type=='text')
    
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
        return rows
    
    # def get_best_row_index(self):
    #     gaps = []
    #     for index, row in enumerate(self.rows):
    #         tokens = sorted(row, key= lambda token: token.left)
    #         if len(tokens) >= 2:
    #             gap = tokens[-1].right - tokens[0].left
    #         else:
    #             gap = 0
    #         gaps.append((index, gap))
    #     return max(gaps, key= lambda gap: gap[1])[0]
    
    @property
    def columns(self):
        inferer = TableInferer(self.filepath, self.page_number)
        column_thresholds = inferer.get_columns()
        table_scale, x_table_scale, y_table_scale = inferer.get_table_scale()
        cropped_scale, x_cropped_scale, y_cropped_scale = inferer.get_cropped_scale()
        table_width, table_height = inferer.image.size
        cropped_width, cropped_height = inferer.cropped_table.size
        table_corners = inferer.table_corners
        print(5)
        
    #     tasks = []
    #     with concurrent.futures.ProcessPoolExecutor(max_workers=100) as executor:
    #         sample_token = self.rows[self.get_best_row_index()][0]
    #         best_y = (sample_token.top + sample_token.bottom) / 2
    #         for index, x in enumerate(numpy.arange(start=self.min_left, stop=self.max_right, step=self.grid_length), start=1):
    #             print(self.collides_with_token(Point(x, best_y)), x)
    #             tasks.append(executor.submit(self.get_y_limits, Point(x, best_y)))
    #         items = [task.result() for task in tasks]
    #     items = self.filter_items(items)
    #     return items
    
    # def filter_items(self, items):
    #     items = sorted(items, key= lambda item: (item[1].length, item[0].x, item[0].y), reverse=True)
    #     filtered_items = []
    #     gone_x = set()
    #     for point, interval in items:
    #         if point.x in gone_x:
    #             continue
    #         else:
    #             filtered_items.append((point, interval))
    #             gone_x.add(point.x)
    #     return filtered_items
    
    # def get_boundaries(self, items):
    #     boundaries = []
    #     MIN_DISTANCE = 0.02
    #     for index, item in enumerate(items, start=1):
    #         if index == 1: 
    #             MAX_HEIGHT = item[1].length
    #         if all(abs(item[0].x - boundary) - MIN_DISTANCE for boundary in boundaries) and item[1].length >= MAX_HEIGHT * 0.8:
    #              boundaries.append(item[0].x)
    #         ...

    def draw_grid(self, pdf_path, output_path):
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            writer = PyPDF2.PdfWriter()

            for page in reader.pages:
                packet = io.BytesIO()
                c = canvas.Canvas(packet, pagesize=letter)
                c.setStrokeColorRGB(1,0,0)
                c.setLineWidth(0.3)

                width, height = letter
                for i in numpy.arange(self.min_left * width, self.max_right * width, self.grid_length * width):
                    for j in numpy.arange(self.min_bottom * height, self.max_top * height, self.grid_length * height):
                        c.rect(i, j, self.grid_length * width, self.grid_length * height, stroke=1, fill=0)
                c.save()
                packet.seek(0)
                new_pdf = PyPDF2.PdfReader(packet)
                page.merge_page(new_pdf.pages[0])
                writer.add_page(page)

        # Write the output PDF
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
    
    def draw_point(self, pdf_path, output_path, point, radius=1):
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            writer = PyPDF2.PdfWriter()

            for page in reader.pages:
                packet = io.BytesIO()
                c = canvas.Canvas(packet, pagesize=letter)
                c.setStrokeColorRGB(1,0,0)
                c.setLineWidth(1)
                c.setFillColor((1,0,0), alpha=0.5)

                # Draw the point
                width, height = letter
                x = point.x * width
                y = point.y * height
                c.circle(x, y, radius, stroke=1, fill=1)

                c.save()
                packet.seek(0)
                new_pdf = PyPDF2.PdfReader(packet)
                page.merge_page(new_pdf.pages[0])
                writer.add_page(page)

        # Write the output PDF
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
    
    # def get_y_limits(self, point):
    #     step = 0.005
    #     max_y = min_y = point.y
    #     while not self.collides_with_token(Point(point.x, max_y)) and max_y <= self.max_top:
    #         max_y += step
    #     while not self.collides_with_token(Point(point.x, min_y)) and min_y >= self.min_bottom:
    #         min_y -= step
    #     return point, pd.Interval(min_y, max_y)
        
    # def collides_with_token(self, point):
    #     tokens_at_point = [token for token in self.tokens if token.left <= point.x <= token.right and token.bottom <= point.y <= token.top]
    #     return bool(tokens_at_point)
    
    def get_closest_token(self, point: Point):
        distances = ((token, math.dist((token.left, token.top), (point.x, point.y))) for token in self.tokens)
        return min(distances, key=lambda x: x[1])

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

class Row(TokenSet):
    ...

class Column(TokenSet):
    ...

class Table(TokenSet):

    def get_dataframe(self):
        cols = {index:list() for index in range(1, len(self.tokens._filter_gaps()) + 2)}
        for row in self.rows:
            text = ''
            for col in cols:
                self.columns
                text = ' '.join(token.text for token in row if token.column==col)
                cols[col].append(text)
        return pd.DataFrame(cols)
    
    def save_csv(self, address:str):
        self.get_dataframe().to_csv(address)