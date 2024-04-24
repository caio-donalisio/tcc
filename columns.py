import torch
from torchvision import transforms
from transformers import AutoModelForObjectDetection, TableTransformerForObjectDetection

from PIL import ImageDraw
from pathlib import Path
from functools import lru_cache
import PyPDF2
from pdf2image import convert_from_bytes
import io
from config import DEBUG_DIR

class MaxResize(object):
    def __init__(self, max_size=800):
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
        self.table_resize = 800
        self.cropped_resize = 1000
    
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

    def draw_grid(self):
        cropped_table_visualized = self.cropped_table.copy()
        draw = ImageDraw.Draw(cropped_table_visualized)
        for cell in self.get_cells():
            draw.rectangle(cell["bbox"], outline="red")
        cropped_table_visualized.save(Path(DEBUG_DIR) / f"GRID_{str(self.filepath).replace('.','')}_{self.page_number:02}.jpg")

    def get_table_scale(self):
        return MaxResize(max_size=self.table_resize).get_scale(self.image)

    def get_cropped_scale(self):
        return MaxResize(max_size=self.cropped_resize).get_scale(self.cropped_table)

    @lru_cache
    def get_cells(self):
        self.image = self.get_page_as_image().convert("RGB")
        objects = self.get_objects(model=AutoModelForObjectDetection.from_pretrained("microsoft/table-transformer-detection", revision="no_timm"), 
                                   image=self.image, 
                                   resize=self.table_resize)
        self.table_corners = objects[0]['bbox']
        tokens = []
        detection_class_thresholds = {
            "table": 0.5,
            "table rotated": 0.5,
            "no object": 10
        }
        crop_padding = 0
        tables_crops = self.objects_to_crops(self.image, tokens, objects, detection_class_thresholds, padding=crop_padding)
        self.cropped_table = tables_crops[0]['image'].convert("RGB")
        self.cropped_size = self.cropped_table.size
        cropped_table_resize = 1000
        cells = self.get_objects(model= TableTransformerForObjectDetection.from_pretrained("microsoft/table-structure-recognition-v1.1-all"), 
                                 image= self.cropped_table, 
                                 resize= cropped_table_resize)
        return cells
    
    def get_objects(self, model, image, resize):
        model.to(self.device)
        detection_transform = transforms.Compose([
            MaxResize(resize),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
        ])
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

    def objects_to_crops(self, img, tokens, objects, class_thresholds, padding=10):
        table_crops = []
        for obj in objects:
            if obj['score'] < class_thresholds[obj['label']]:
                continue

            cropped_table = {}

            bbox = obj['bbox']
            bbox = [bbox[0]-padding, bbox[1]-padding, bbox[2]+padding, bbox[3]+padding]

            cropped_img = img.crop(bbox)

            table_tokens = [token for token in tokens]# if iob(token['bbox'], bbox) >= 0.5]
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

            cropped_table['image'] = cropped_img
            cropped_table['tokens'] = table_tokens

            table_crops.append(cropped_table)

        return table_crops

    def filter_close_values(self, values, threshold=7):
        filtered_values = [values[0]]
        for i in range(1, len(values)):
            if abs(values[i] - filtered_values[-1]) > threshold:
                filtered_values.append(values[i])
        return filtered_values

    @lru_cache
    def get_columns(self):
        edges = []
        if self.get_cells():
            edges = sorted([cell['bbox'][2] for cell in self.get_cells()])
            edges = self.filter_close_values(edges)
            if len(edges) > 1:
                edges.pop(-1)
            self.draw_grid()
        return edges

    @lru_cache
    def get_rows(self):
        edges = []
        if self.get_cells():
            edges = sorted([cell['bbox'][3] for cell in self.get_cells()])
            edges = self.filter_close_values(edges)
            if len(edges) > 1:
                edges.pop(-1)
            self.draw_grid()
        return edges

