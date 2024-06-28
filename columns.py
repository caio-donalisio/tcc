import torch
from torchvision import transforms
from transformers import AutoModelForObjectDetection, TableTransformerForObjectDetection

from PIL import ImageDraw, Image, ImageEnhance
from pathlib import Path
from functools import lru_cache
import PyPDF2
from pdf2image import convert_from_bytes
import io
import config
import logging

logger = logging.getLogger("table_generator")

RED = (255,0,0,200)

class MaxResize(object):
    def __init__(self, max_size):
        self.max_size = max_size

    def __call__(self, image):
        scale, x_scale, y_scale = self.get_scale(image)
        resized_image = image.resize((x_scale, y_scale))
        return resized_image
    
    def get_scale(self, image):
        width, height = image.size
        current_max_size = max(width, height)
        scale = self.max_size / current_max_size
        x_scale, y_scale = int(round(scale * width)), int(round(scale * height))
        return scale, x_scale, y_scale

class TableInferer:
    
    def __init__(self, filepath, page_number):
        self.filepath = Path(filepath)
        self.page_number = page_number
        self.image = self.get_page_as_image().convert("RGB")
        self.rotated = False
        self.get_cells()
        self.draw_grid()
        
    def get_page_as_image(self):
        reader = PyPDF2.PdfReader(self.filepath)
        writer = PyPDF2.PdfWriter()
        page = reader.pages[self.page_number - 1]
        writer.add_page(page)
        buf = io.BytesIO()
        writer.write(buf)
        buf.seek(0)
        image = convert_from_bytes(buf.read())
        return image[0]

    def preprocess_image(self, image):
        ... # Preprocessing function, if needed (e.g. change contrast)
        return image

    def draw_grid(self):
        image = self.image.copy()
        draw = ImageDraw.Draw(image, 'RGBA')
        if getattr(self, 'table_corners', None):
            for column_pack, corner_set in zip(self.get_columns(), self.table_corners):
                draw.rectangle(corner_set, outline="red", width=5)
                for column in column_pack:
                    draw.line(
                        [(column * image.width, corner_set[1]), (column * image.width, corner_set[3])], 
                        fill=RED, 
                        width=3)
            image.save(Path(config.DEBUG_GRID_FILES_DIR) / f"{self.filepath.with_suffix('').name}_{self.page_number:04}.jpg")

    def get_table_scale(self):
        return MaxResize(max_size=config.TABLE_RESIZE).get_scale(self.image)

    def get_cropped_scale(self):
        return MaxResize(max_size=config.CROPPED_RESIZE).get_scale(self.cropped_table)

    @lru_cache
    def get_cells(self):
        
        self.table_corners = []
        self.cropped_table = []
        self.cropped_size = []
        
        self.image = self.preprocess_image(self.image)
        objects = self.get_objects(model=AutoModelForObjectDetection.from_pretrained("microsoft/table-transformer-detection", revision="no_timm"), 
                                   image=self.image, 
                                   resize=config.TABLE_RESIZE)
        cell_pack = []
        for obj in objects:
            self.table_corners.append(obj['bbox'])
            tokens = []
            table_crops = self.objects_to_crops(img=self.image, tokens=tokens, objects=objects, 
                                                class_thresholds=config.DETECTION_CLASS_THRESHOLDS, padding=config.CROP_PADDING)
            for table in table_crops:
                cropped_table = table['image'].convert("RGB")
                self.cropped_table.append(table['image'].convert("RGB"))
                self.cropped_size.append(self.cropped_table[-1].size)
                cells = self.get_objects(model= TableTransformerForObjectDetection.from_pretrained("microsoft/table-structure-recognition-v1.1-all"), 
                                    image= cropped_table, 
                                    resize= config.CROPPED_RESIZE)
                if cells not in cell_pack: 
                    cell_pack.append(cells)
        if not cell_pack: 
            logger.warn(f'Could not extract table from file {self.filepath.name} - page {self.page_number}')

        return cell_pack
    
    def get_objects(self, model, image, resize):
        model.to(self.device)
        detection_transform = transforms.Compose([
            MaxResize(resize),
            transforms.ToTensor(),
            transforms.Normalize(*config.NORMALIZE_VECTORS)])
        pixel_values = detection_transform(image).unsqueeze(0)
        pixel_values = pixel_values.to(self.device)
        with torch.no_grad():
          outputs = model(pixel_values)
        id2label = model.config.id2label
        id2label[len(model.config.id2label)] = "no object"
        objects = self.outputs_to_objects(outputs, image.size, id2label)
        return objects
    
    @property
    def device(self):
        return "cuda" if torch.cuda.is_available() else "cpu"
    
    def box_cxcywh_to_xyxy(self, x):
        x_c, y_c, w, h = x.unbind(-1)
        b = [(x_c - 0.5 * w), (y_c - 0.5 * h), (x_c + 0.5 * w), (y_c + 0.5 * h)]
        return torch.stack(b, dim=1)

    def rescale_bboxes(self, out_bbox, size):
        img_w, img_h = size
        b = self.box_cxcywh_to_xyxy(out_bbox)
        b = b * torch.tensor([img_w, img_h, img_w, img_h], dtype=torch.float32)
        return b

    def outputs_to_objects(self, outputs, img_size, id2label):
        m = outputs.logits.softmax(-1).max(-1)
        pred_labels = list(m.indices.detach().cpu().numpy())[0]
        pred_scores = list(m.values.detach().cpu().numpy())[0]
        pred_bboxes = outputs['pred_boxes'].detach().cpu()[0]
        pred_bboxes = [elem.tolist() for elem in self.rescale_bboxes(pred_bboxes, img_size)]

        objects = []
        for label, score, bbox in zip(pred_labels, pred_scores, pred_bboxes):
            class_label = id2label[int(label)]
            if not class_label == 'no object':
                objects.append({'label': class_label, 'score': float(score),
                                'bbox': [float(elem) for elem in bbox]})
        return objects

    def objects_to_crops(self, img, tokens, objects, class_thresholds, padding):
        table_crops = []
        for obj in objects:
            if obj['score'] < class_thresholds[obj['label']]:
                continue

            cropped_table = {}

            bbox = obj['bbox']
            bbox = [bbox[0]-padding, bbox[1]-padding, bbox[2]+padding, bbox[3]+padding]

            cropped_img = img.crop(bbox)

            table_tokens = [token for token in tokens if token['score'] >= config.SCORE_THRESHOLD]
            for token in table_tokens:
                token['bbox'] = [token['bbox'][0]-bbox[0],
                                token['bbox'][1]-bbox[1],
                                token['bbox'][2]-bbox[0],
                                token['bbox'][3]-bbox[1]]

            # If table is predicted to be rotated, rotate cropped image and tokens/words:
            if obj['label'] == 'table rotated':
                cropped_img = cropped_img.rotate(270, expand=True)
                for token in table_tokens:
                    bbox = token['bbox']
                    bbox = [cropped_img.size[0]-bbox[3]-1,
                            bbox[0],
                            cropped_img.size[0]-bbox[1]-1,
                            bbox[2]]
                    token['bbox'] = bbox
                self.rotated = True
            cropped_table['image'] = cropped_img
            cropped_table['tokens'] = table_tokens

            table_crops.append(cropped_table)

        return table_crops

    def filter_close_values(self, values):
        filtered_values = [values[0]] if values else []
        for i in range(1, len(values)):
            if abs(values[i] - filtered_values[-1]) > config.SIMILARITY_THRESHOLD:
                filtered_values.append(values[i])
        return filtered_values

    @lru_cache
    def get_features(self):
        edges = []
        bbox_index = 3 if self.rotated else 2
        feature_name = 'table row' if self.rotated else 'table column'
        edges_pack = []
        for cells in self.get_cells():
            edges = sorted([cell['bbox'][bbox_index] for cell in cells if cell['label'] == feature_name])
            edges = self.filter_close_values(edges)
            # if len(edges) > 1:
            #     edges.pop(-1)
            edges_pack.append(edges)
        return edges_pack

    @lru_cache
    def get_columns(self):
        columns_pack = []
        for index, lines in enumerate(self.get_features()):
            # for lines in pack:
            table_width, _ = self.image.size
            cropped_width, _ = self.cropped_table[index].size
            table_x_offset, *_ = self.table_corners[index]
            column_xs = [( table_x_offset / table_width ) + ( (x / cropped_width) * ( cropped_width / table_width) ) for x in lines]
            columns_pack.append(column_xs)
        return columns_pack

    # TODO: Implement get_rows
    # @lru_cache
    # def get_rows(self):
    #     if lines := self.get_lines(bbox_index=3):
    #         table_width, _ = self.image.size
    #         cropped_width, _ = self.cropped_table.size
    #         table_x_offset, *_ = self.table_corners
    #         column_xs = [( table_x_offset / table_width ) + ( (x / cropped_width) * ( cropped_width / table_width) ) for x in lines]
    #         return column_xs
    #     else:
    #         return []
    